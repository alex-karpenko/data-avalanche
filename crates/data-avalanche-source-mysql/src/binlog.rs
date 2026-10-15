//! Binlog primitives the stream builds on, each pure and unit-tested (S-MY10, D-V1.6):
//!
//! - [`rotate_name`]: MariaDB's fake ROTATE keeps its CRC on the file name;
//! - [`MariaGtidEvent`]: MariaDB's `GTID_EVENT` (type 162), which `mysql_common` doesn't read;
//! - [`Offsets`]: the end offset of each event in its file (the `_seq` order key, Q-M7-4) and the
//!   check that a compressed transaction yields all of its events (`BINLOG_PAYLOAD_INVALID`);
//! - [`decompress`]: MariaDB's compressed events (`log_bin_compress`, Q-M7a-5), which
//!   `mysql_common` reads as nothing.

use std::io::Read;

use data_avalanche_core::position::MariaGtid;
use mysql_async::binlog::events::{Event, EventData};

/// Raw event types the stream tells apart (MySQL `binlog_event.h`, MariaDB `log_event.h`).
pub mod types {
    pub const QUERY: u8 = 2;
    pub const STOP: u8 = 3;
    pub const ROTATE: u8 = 4;
    pub const INTVAR: u8 = 5;
    pub const RAND: u8 = 13;
    pub const USER_VAR: u8 = 14;
    pub const FORMAT_DESCRIPTION: u8 = 15;
    pub const XID: u8 = 16;
    pub const TABLE_MAP: u8 = 19;
    pub const WRITE_ROWS_V1: u8 = 23;
    pub const UPDATE_ROWS_V1: u8 = 24;
    pub const DELETE_ROWS_V1: u8 = 25;
    pub const INCIDENT: u8 = 26;
    pub const HEARTBEAT: u8 = 27;
    pub const IGNORABLE: u8 = 28;
    pub const ROWS_QUERY: u8 = 29;
    pub const WRITE_ROWS: u8 = 30;
    pub const UPDATE_ROWS: u8 = 31;
    pub const DELETE_ROWS: u8 = 32;
    pub const GTID: u8 = 33;
    pub const ANONYMOUS_GTID: u8 = 34;
    pub const PREVIOUS_GTIDS: u8 = 35;
    pub const TRANSACTION_CONTEXT: u8 = 36;
    pub const VIEW_CHANGE: u8 = 37;
    pub const XA_PREPARE: u8 = 38;
    pub const PARTIAL_UPDATE_ROWS: u8 = 39;
    pub const TRANSACTION_PAYLOAD: u8 = 40;
    pub const HEARTBEAT_V2: u8 = 41;
    pub const GTID_TAGGED: u8 = 42;
    pub const MARIADB_ANNOTATE_ROWS: u8 = 160;
    pub const MARIADB_BINLOG_CHECKPOINT: u8 = 161;
    pub const MARIADB_GTID: u8 = 162;
    pub const MARIADB_GTID_LIST: u8 = 163;
    pub const MARIADB_START_ENCRYPTION: u8 = 164;
    pub const MARIADB_QUERY_COMPRESSED: u8 = 165;
    pub const MARIADB_WRITE_ROWS_COMPRESSED_V1: u8 = 166;
    pub const MARIADB_UPDATE_ROWS_COMPRESSED_V1: u8 = 167;
    pub const MARIADB_DELETE_ROWS_COMPRESSED_V1: u8 = 168;
}

/// A ROTATE event's file name. MariaDB's fake ROTATE at the start of a dump comes before the
/// format description, so `mysql_common` doesn't know the checksum is on and leaves the 4 CRC32
/// bytes on the name (S-MY10b): strip them when the name only reads as `<base>.<6 digits>`
/// without them.
pub fn rotate_name(raw: &[u8]) -> &[u8] {
    fn file_name(n: &[u8]) -> bool {
        n.len() > 7 && n[n.len() - 7] == b'.' && n[n.len() - 6..].iter().all(u8::is_ascii_digit)
    }
    match raw.len().checked_sub(4) {
        Some(cut) if !file_name(raw) && file_name(&raw[..cut]) => &raw[..cut],
        _ => raw,
    }
}

/// MariaDB's `GTID_EVENT` (type 162): `seq_no` u64 LE, `domain_id` u32 LE, `flags2` u8; the
/// server id is the event header's. It opens a transaction, or a standalone statement (DDL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaGtidEvent {
    pub gtid: MariaGtid,
    pub flags: u8,
}

impl MariaGtidEvent {
    /// The statement stands alone: no `BEGIN`, no `XID` (DDL).
    pub const STANDALONE: u8 = 0x01;
    /// The XA transaction's prepare, and its later commit or rollback.
    pub const PREPARED_XA: u8 = 0x40;
    pub const COMPLETED_XA: u8 = 0x80;

    /// From the event body (checksum already off) and the header's server id.
    pub fn parse(server_id: u32, data: &[u8]) -> Result<Self, String> {
        if data.len() < 13 {
            return Err(format!("a MariaDB GTID event of {} bytes", data.len()));
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().expect("8 bytes"));
        let domain = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
        Ok(Self {
            gtid: MariaGtid {
                domain,
                server_id,
                seq,
            },
            flags: data[12],
        })
    }

    pub fn standalone(&self) -> bool {
        self.flags & Self::STANDALONE != 0
    }

    /// An XA transaction's prepare or completion: its rows can still roll back after the event.
    pub fn xa(&self) -> bool {
        self.flags & (Self::PREPARED_XA | Self::COMPLETED_XA) != 0
    }
}

/// The end offset of each event in its binlog file (Q-M7-4): the header's `log_pos` when the
/// server set it, else the last end plus the event's size. MySQL leaves `log_pos` at 0 inside a
/// compressed transaction and MariaDB 11.x on table maps and rows events (S-MY10f): the computed
/// offsets still grow, and equal the header's wherever it is set. Events inside a
/// `TRANSACTION_PAYLOAD` take the payload's end, and count its `uncompressed_size` down: an event
/// with a set `log_pos` (the next transaction) or a heartbeat before the count reaches zero means
/// the payload's events were lost (D-V1.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offsets {
    file: String,
    end: u64,
    /// Bytes of the current payload's events still to come.
    inner: u64,
    /// The payload's `uncompressed_size`, for the message.
    payload: u64,
}

impl Offsets {
    /// At `pos` in `file`: where a dump starts, before its fake ROTATE.
    pub fn new(file: &str, pos: u64) -> Self {
        Self {
            file: file.to_owned(),
            end: pos,
            inner: 0,
            payload: 0,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    /// The end offset of the last event.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// True between a payload event and its last inner event.
    pub fn in_payload(&self) -> bool {
        self.inner > 0
    }

    fn lost(&self) -> String {
        format!(
            "the compressed transaction ending at {}:{} yielded {} of its {} bytes of events",
            self.file,
            self.end,
            self.payload - self.inner,
            self.payload
        )
    }

    /// A ROTATE (real or fake): the next events are in `file` from `pos`.
    pub fn rotate(&mut self, file: &str, pos: u64) -> Result<(), String> {
        if self.in_payload() {
            return Err(self.lost());
        }
        self.file = file.to_owned();
        self.end = pos;
        Ok(())
    }

    /// A heartbeat: it carries no event of the file, and can't come inside a payload.
    pub fn heartbeat(&self) -> Result<(), String> {
        match self.in_payload() {
            true => Err(self.lost()),
            false => Ok(()),
        }
    }

    /// An event of the file with header `log_pos` and `size`: its end offset.
    pub fn event(&mut self, log_pos: u32, size: u32) -> Result<u64, String> {
        let size = u64::from(size);
        if self.in_payload() {
            if log_pos != 0 {
                return Err(self.lost());
            }
            if size > self.inner {
                return Err(format!(
                    "an event of {size} bytes overruns the compressed transaction ending at {}:{} ({} bytes left)",
                    self.file, self.end, self.inner
                ));
            }
            self.inner -= size;
            return Ok(self.end);
        }
        self.end = match log_pos {
            0 => self.end + size,
            set => u64::from(set),
        };
        Ok(self.end)
    }

    /// After [`Offsets::event`] for a `TRANSACTION_PAYLOAD`: its events, `uncompressed_size`
    /// bytes, follow. A payload without events is invalid.
    pub fn payload(&mut self, uncompressed_size: u64) -> Result<(), String> {
        if uncompressed_size == 0 {
            return Err(format!(
                "the compressed transaction ending at {}:{} holds no events",
                self.file, self.end
            ));
        }
        self.inner = uncompressed_size;
        self.payload = uncompressed_size;
        Ok(())
    }
}

/// The uncompressed type of a MariaDB compressed event the source reads (measured on 11.8 and
/// 10.11: rows events v1, and statements). The v2 rows forms (169–171) were never seen; they
/// halt the pipeline instead.
pub fn plain_type(raw: u8) -> Option<u8> {
    use types::*;
    Some(match raw {
        MARIADB_QUERY_COMPRESSED => QUERY,
        MARIADB_WRITE_ROWS_COMPRESSED_V1 => WRITE_ROWS_V1,
        MARIADB_UPDATE_ROWS_COMPRESSED_V1 => UPDATE_ROWS_V1,
        MARIADB_DELETE_ROWS_COMPRESSED_V1 => DELETE_ROWS_V1,
        _ => return None,
    })
}

/// MariaDB's compressed block: a header byte (`0x80` | the length of the length), the
/// uncompressed length big-endian, then zlib.
fn inflate(block: &[u8]) -> Result<Vec<u8>, String> {
    let header = *block.first().ok_or("an empty compressed block")?;
    let lenlen = usize::from(header & 0x07);
    if header & 0x80 == 0 || !(1..=4).contains(&lenlen) || block.len() <= 1 + lenlen {
        return Err(format!("a compressed block with header {header:#04x}"));
    }
    let len = block[1..1 + lenlen]
        .iter()
        .fold(0usize, |a, b| (a << 8) | usize::from(*b));
    let mut out = Vec::with_capacity(len.min(1 << 20));
    flate2::read::ZlibDecoder::new(&block[1 + lenlen..])
        .take(len as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("zlib: {e}"))?;
    if out.len() != len {
        return Err(format!(
            "inflated {} bytes, the header says {len}",
            out.len()
        ));
    }
    Ok(out)
}

/// `e`'s header with type `ty` and body `data`, read under `e`'s format description (with a
/// placeholder checksum when the format has one: reading doesn't verify it).
fn rebuild(e: &Event, ty: u8, data: &[u8]) -> Result<Event, String> {
    let h = e.header();
    let footer = e.fde().footer();
    let checksum = footer.get_checksum_enabled()
        && footer
            .get_checksum_alg()
            .ok()
            .flatten()
            .is_some_and(|a| a as u8 != 0);
    let size = 19 + data.len() + if checksum { 4 } else { 0 };
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(&h.timestamp().to_le_bytes());
    buf.push(ty);
    buf.extend_from_slice(&h.server_id().to_le_bytes());
    buf.extend_from_slice(
        &u32::try_from(size)
            .map_err(|e| e.to_string())?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&h.log_pos().to_le_bytes());
    buf.extend_from_slice(&h.flags_raw().to_le_bytes());
    buf.extend_from_slice(data);
    if checksum {
        buf.extend_from_slice(&[0; 4]);
    }
    Event::read(e.fde(), &buf[..]).map_err(|e| e.to_string())
}

/// A MariaDB compressed event as its uncompressed form: read under the plain type, the event
/// names where its compressed tail starts (the rows, or the statement text); inflated, it is
/// read again. `None` for an event that isn't one.
pub fn decompress(e: &Event) -> Result<Option<Event>, String> {
    let Some(ty) = plain_type(e.header().event_type_raw()) else {
        return Ok(None);
    };
    let shell = rebuild(e, ty, e.data())?;
    let tail = match shell.read_data().map_err(|e| e.to_string())? {
        Some(EventData::QueryEvent(q)) => q.query_raw().len(),
        Some(EventData::RowsEvent(r)) => r.rows_data().len(),
        other => return Err(format!("a compressed event reads as {other:?}")),
    };
    let data = e.data();
    let (head, compressed) = data.split_at(data.len() - tail);
    let mut body = head.to_vec();
    body.extend(inflate(compressed)?);
    rebuild(e, ty, &body).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::binlog::events::RowsEventData;
    use mysql_async::binlog::value::BinlogValue;
    use mysql_common::binlog::EventStreamReader;
    use mysql_common::binlog::consts::BinlogVersion;

    /// Events captured from MariaDB 11.8 with `log_bin_compress = ON` (`m7/x` capture, 2026-10-14):
    /// the format description, a table map of `m7v (id INT PRIMARY KEY, v VARCHAR(40))`, the
    /// compressed insert of `(7, 'compressed rows, decompressed')`, a GTID event and the
    /// compressed `ALTER TABLE m7v COMMENT 'a compressed statement'`.
    const FDE: &str = "8223cf6a0f01000000fc000000000000000000040031312e382e392d4d6172696144422d756275323430342d6c6f6700\
        00000000000000000000000000000000000000000000000000000013380d000800120004040404120000e400041a0800\
        0000080808020000000a0a0a0000000000000a0a0a000000000000000000000000000000000000000000000000000000\
        000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\
        000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004130400\
        0d0808080a0a0a0110dbbc0e";
    const GTID_ROWS: &str =
        "f567cf6aa2010000002a0000001820000008006801000000000000000000000c109441000000916d1eee";
    const TABLE_MAP: &str = "f567cf6a1301000000440000000000000000002d000000000001000570726f626500036d37760002030f02a000020101\
        000203fc000904050269640176080100d9f8cd79";
    const WRITE_COMPRESSED: &str = "f567cf6aa601000000460000000000000000002d0000000000010002038123789cfbc3cec0c0209b9c9f5b50945a5c9c\
        9aa250945f5eaca390928a100200d2430c8b8642f8f5";
    const GTID_DDL: &str =
        "f567cf6aa2010000002a0000003d210000080069010000000000000000000029109441000000fc0a44cb";
    const QUERY_COMPRESSED: &str = "f567cf6aa50100000087000000c421000000009441000000000000050000230000000000010100002054000000000603\
        7374640400090009000981c28300000000000070726f6265008130789c73f409710d52087174f27155c8352f5370f6f7\
        f575f50b51504f5448cecf2d284a2d2e4e4d51282e492c49cd4dcd2b5107005cf70f879bee9f57";

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn reader() -> EventStreamReader {
        let mut r = EventStreamReader::new(BinlogVersion::Version4);
        r.read(&bytes(FDE)[..])
            .unwrap()
            .expect("the format description");
        r
    }

    fn event(r: &mut EventStreamReader, hex: &str) -> Event {
        r.read(&bytes(hex)[..]).unwrap().expect("an event")
    }

    #[test]
    fn a_rotate_name_loses_the_crc_mariadb_leaves_on_it() {
        assert_eq!(
            rotate_name(b"mariadb-bin.000037x\x1b\xfe\x01"),
            b"mariadb-bin.000037"
        );
        assert_eq!(rotate_name(b"mysql-bin.000003"), b"mysql-bin.000003");
        // A name that reads either way is left alone.
        assert_eq!(rotate_name(b"b.000001.000002"), b"b.000001.000002");
        assert_eq!(rotate_name(b"abc"), b"abc");
    }

    #[test]
    fn a_mariadb_gtid_event_reads_its_gtid_and_flags() {
        let mut r = reader();
        let e = event(&mut r, GTID_ROWS);
        let g = MariaGtidEvent::parse(e.header().server_id(), e.data()).unwrap();
        assert_eq!(
            g.gtid,
            MariaGtid {
                domain: 0,
                server_id: 1,
                seq: 360
            }
        );
        assert!(!g.standalone() && !g.xa());
        let e = event(&mut r, GTID_DDL);
        let g = MariaGtidEvent::parse(1, e.data()).unwrap();
        assert_eq!(g.gtid.seq, 361);
        assert!(g.standalone(), "DDL stands alone: {:#x}", g.flags);
        let xa = MariaGtidEvent {
            flags: MariaGtidEvent::PREPARED_XA,
            ..g
        };
        assert!(xa.xa());
        assert!(MariaGtidEvent::parse(1, &[0; 12]).is_err());
    }

    #[test]
    fn offsets_follow_the_header_or_add_up_sizes() {
        let mut o = Offsets::new("b.000001", 4);
        o.rotate("b.000001", 4).unwrap();
        assert_eq!(o.event(0, 120).unwrap(), 124, "no position set: the sum");
        assert_eq!(o.event(300, 50).unwrap(), 300, "a set position wins");
        assert_eq!(o.event(0, 40).unwrap(), 340);
        o.heartbeat().unwrap();
        o.rotate("b.000002", 4).unwrap();
        assert_eq!((o.file(), o.end()), ("b.000002", 4));
    }

    #[test]
    fn a_payloads_events_take_its_end_and_count_down() {
        let mut o = Offsets::new("b.000001", 4);
        assert_eq!(o.event(1000, 400).unwrap(), 1000);
        o.payload(250).unwrap();
        assert!(o.in_payload());
        for size in [100, 100, 50] {
            assert_eq!(o.event(0, size).unwrap(), 1000, "inside: the payload's end");
        }
        assert!(!o.in_payload());
        assert_eq!(o.event(1100, 100).unwrap(), 1100);
    }

    #[test]
    fn a_payload_whose_events_went_missing_is_invalid() {
        let lost = |o: &mut Offsets| o.payload(250).map(|_| ());
        // Nothing came out: the next transaction's event has its position set.
        let mut o = Offsets::new("b.000001", 4);
        o.event(1000, 400).unwrap();
        lost(&mut o).unwrap();
        let e = o.event(1100, 100).unwrap_err();
        assert!(e.contains("yielded 0 of its 250 bytes"), "{e}");
        // Part came out, then a heartbeat, or a rotate.
        let mut o = Offsets::new("b.000001", 4);
        o.event(1000, 400).unwrap();
        lost(&mut o).unwrap();
        o.event(0, 100).unwrap();
        assert!(
            o.heartbeat()
                .unwrap_err()
                .contains("yielded 100 of its 250")
        );
        assert!(o.rotate("b.000002", 4).is_err());
        // An event larger than what is left, and a payload without events.
        assert!(o.event(0, 200).unwrap_err().contains("overruns"));
        assert!(Offsets::new("b.000001", 4).payload(0).is_err());
    }

    #[test]
    fn mariadb_compressed_rows_read_as_their_plain_event() {
        let mut r = reader();
        let _ = event(&mut r, GTID_ROWS);
        let map = event(&mut r, TABLE_MAP);
        let compressed = event(&mut r, WRITE_COMPRESSED);
        assert!(
            compressed.read_data().unwrap().is_none(),
            "mysql_common reads nothing"
        );
        let plain = decompress(&compressed)
            .unwrap()
            .expect("a compressed event");
        assert_eq!(plain.header().event_type_raw(), types::WRITE_ROWS_V1);
        assert_eq!(plain.header().log_pos(), compressed.header().log_pos());
        let Some(EventData::TableMapEvent(tme)) = map.read_data().unwrap() else {
            panic!("a table map")
        };
        let Some(EventData::RowsEvent(RowsEventData::WriteRowsEventV1(w))) =
            plain.read_data().unwrap()
        else {
            panic!("a write rows event")
        };
        let rows: Vec<_> = RowsEventData::WriteRowsEventV1(w)
            .rows(&tme)
            .map(|r| r.unwrap().1.unwrap().unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                BinlogValue::Value(mysql_async::Value::Int(7)),
                BinlogValue::Value(mysql_async::Value::Bytes(
                    b"compressed rows, decompressed".to_vec()
                )),
            ]
        );
    }

    #[test]
    fn a_mariadb_compressed_statement_reads_as_its_query() {
        let mut r = reader();
        let compressed = event(&mut r, QUERY_COMPRESSED);
        let plain = decompress(&compressed).unwrap().unwrap();
        let Some(EventData::QueryEvent(q)) = plain.read_data().unwrap() else {
            panic!("a query event")
        };
        assert_eq!(
            q.query(),
            "ALTER TABLE m7v COMMENT 'a compressed statement'"
        );
        assert_eq!(q.schema(), "probe");
        // Not a compressed event: nothing to do.
        assert!(decompress(&event(&mut r, GTID_DDL)).unwrap().is_none());
    }

    #[test]
    fn a_corrupt_compressed_block_is_an_error() {
        assert!(inflate(&[]).is_err());
        assert!(inflate(&[0x01, 5, 0]).unwrap_err().contains("header"));
        assert!(
            inflate(&[0x81, 5, 0x78, 0x9c, 0])
                .unwrap_err()
                .contains("zlib")
        );
    }

    #[test]
    fn a_header_claiming_more_than_the_block_inflates_is_refused() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"abc").unwrap();
        let compressed = encoder.finish().unwrap();

        let mut block = vec![0x84, 0xff, 0xff, 0xff, 0xff];
        block.extend(compressed);

        assert!(
            inflate(&block)
                .unwrap_err()
                .contains("the header says 4294967295")
        );
    }

    #[test]
    fn a_block_inflating_past_its_header_is_refused() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&vec![0u8; 1000]).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut block = vec![0x81, 3];
        block.extend(compressed);

        assert!(inflate(&block).unwrap_err().contains("the header says 3"));
    }
}
