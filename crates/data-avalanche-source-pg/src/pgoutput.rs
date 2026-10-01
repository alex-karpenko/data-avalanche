//! The pgoutput protocol v1 decoder (PostgreSQL docs, "Logical Replication Message Formats"): the
//! part `pgwire-replication` leaves to the caller. The crate itself turns Begin (`B`), Commit (`C`)
//! and logical messages (`M`) into events; this decodes the rest of an `XLogData` payload. Tuples
//! are text (Q-M5-3): a binary datum is a protocol error.

use bytes::Bytes;

/// One column value of a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    Null,
    /// An unchanged TOASTed value the server didn't send (`u`).
    Unchanged,
    /// The value's text form, in the database encoding.
    Text(Bytes),
}

/// A column of a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    /// Part of the replica identity (flag bit 1).
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
}

/// Which old tuple an Update or Delete carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldKind {
    /// `K`: the replica identity columns; the other columns are NULL.
    Key,
    /// `O`: the whole old row (`REPLICA IDENTITY FULL`).
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Relation {
        oid: u32,
        /// Empty for `pg_catalog`.
        namespace: String,
        name: String,
        /// `d` default, `n` nothing, `f` full, `i` index.
        replica_identity: u8,
        columns: Vec<RelationColumn>,
    },
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Origin {
        commit_lsn: u64,
        name: String,
    },
    Insert {
        relation: u32,
        new: Vec<Datum>,
    },
    Update {
        relation: u32,
        old: Option<(OldKind, Vec<Datum>)>,
        new: Vec<Datum>,
    },
    Delete {
        relation: u32,
        old: (OldKind, Vec<Datum>),
    },
    Truncate {
        /// Bit 1 `CASCADE`, bit 2 `RESTART IDENTITY`.
        options: u8,
        relations: Vec<u32>,
    },
}

struct Reader<'a> {
    data: &'a Bytes,
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize, what: &str) -> Result<&[u8], String> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| format!("{what}: the message ends at byte {}", self.data.len()))?;
        let s = &self.data[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn u8(&mut self, what: &str) -> Result<u8, String> {
        Ok(self.take(1, what)?[0])
    }

    fn i16(&mut self, what: &str) -> Result<i16, String> {
        Ok(i16::from_be_bytes(
            self.take(2, what)?.try_into().expect("2 bytes"),
        ))
    }

    fn i32(&mut self, what: &str) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            self.take(4, what)?.try_into().expect("4 bytes"),
        ))
    }

    fn u32(&mut self, what: &str) -> Result<u32, String> {
        Ok(u32::from_be_bytes(
            self.take(4, what)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self, what: &str) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8, what)?.try_into().expect("8 bytes"),
        ))
    }

    fn string(&mut self, what: &str) -> Result<String, String> {
        let rest = &self.data[self.at..];
        let end = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| format!("{what}: no NUL terminator"))?;
        let s = std::str::from_utf8(&rest[..end])
            .map_err(|_| format!("{what}: not UTF-8"))?
            .to_owned();
        self.at += end + 1;
        Ok(s)
    }

    /// A capacity for `n` items of at least `size` bytes each: never more than the bytes left can
    /// hold, so a corrupt count can't allocate.
    fn capacity(&self, n: i64, size: usize) -> usize {
        let left = (self.data.len() - self.at) / size;
        usize::try_from(n).unwrap_or(0).min(left)
    }

    fn tuple(&mut self) -> Result<Vec<Datum>, String> {
        let n = self.i16("tuple column count")?;
        let mut out = Vec::with_capacity(self.capacity(n.into(), 1));
        for i in 0..n.max(0) {
            out.push(match self.u8("tuple column kind")? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                b't' => {
                    let len = self.i32("text length")?;
                    let len = usize::try_from(len).map_err(|_| format!("text length {len}"))?;
                    let start = self.at;
                    self.take(len, "text value")?;
                    Datum::Text(self.data.slice(start..start + len))
                }
                b'b' => return Err(format!("column {i} is binary; the stream asks for text")),
                k => return Err(format!("unknown tuple column kind {:?}", k as char)),
            });
        }
        Ok(out)
    }

    fn old_kind(k: u8) -> Option<OldKind> {
        match k {
            b'K' => Some(OldKind::Key),
            b'O' => Some(OldKind::Full),
            _ => None,
        }
    }
}

/// Decodes one `XLogData` payload. Every byte must belong to the message.
pub fn decode(data: &Bytes) -> Result<Message, String> {
    let mut r = Reader { data, at: 0 };
    let tag = r.u8("message tag")?;
    let msg = match tag {
        b'R' => {
            let oid = r.u32("relation oid")?;
            let namespace = r.string("namespace")?;
            let name = r.string("relation name")?;
            let replica_identity = r.u8("replica identity")?;
            let n = r.i16("column count")?;
            let mut columns = Vec::with_capacity(r.capacity(n.into(), 10));
            for _ in 0..n {
                let flags = r.u8("column flags")?;
                columns.push(RelationColumn {
                    key: flags & 1 == 1,
                    name: r.string("column name")?,
                    type_oid: r.u32("column type")?,
                    typmod: r.i32("column typmod")?,
                });
            }
            Message::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            }
        }
        b'Y' => Message::Type {
            oid: r.u32("type oid")?,
            namespace: r.string("type namespace")?,
            name: r.string("type name")?,
        },
        b'O' => Message::Origin {
            commit_lsn: r.u64("origin commit LSN")?,
            name: r.string("origin name")?,
        },
        b'I' => {
            let relation = r.u32("relation oid")?;
            match r.u8("insert tuple kind")? {
                b'N' => {}
                k => return Err(format!("Insert: tuple kind {:?}, expected 'N'", k as char)),
            }
            Message::Insert {
                relation,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let relation = r.u32("relation oid")?;
            let mut k = r.u8("update tuple kind")?;
            let old = match Reader::old_kind(k) {
                Some(kind) => {
                    let t = r.tuple()?;
                    k = r.u8("update tuple kind")?;
                    Some((kind, t))
                }
                None => None,
            };
            if k != b'N' {
                return Err(format!("Update: tuple kind {:?}, expected 'N'", k as char));
            }
            Message::Update {
                relation,
                old,
                new: r.tuple()?,
            }
        }
        b'D' => {
            let relation = r.u32("relation oid")?;
            let k = r.u8("delete tuple kind")?;
            let kind = Reader::old_kind(k).ok_or_else(|| {
                format!("Delete: tuple kind {:?}, expected 'K' or 'O'", k as char)
            })?;
            Message::Delete {
                relation,
                old: (kind, r.tuple()?),
            }
        }
        b'T' => {
            let n = r.i32("truncate relation count")?;
            let options = r.u8("truncate options")?;
            let mut relations = Vec::with_capacity(r.capacity(n.into(), 4));
            for _ in 0..n {
                relations.push(r.u32("truncated relation oid")?);
            }
            Message::Truncate { options, relations }
        }
        t => return Err(format!("unexpected message tag {:?}", t as char)),
    };
    if r.at != data.len() {
        return Err(format!(
            "{:?} message: {} bytes left over",
            tag as char,
            data.len() - r.at
        ));
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds messages byte by byte, as the server writes them.
    #[derive(Default)]
    struct W(Vec<u8>);

    impl W {
        fn u8(mut self, v: u8) -> Self {
            self.0.push(v);
            self
        }
        fn i16(mut self, v: i16) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn u64(mut self, v: u64) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn s(mut self, v: &str) -> Self {
            self.0.extend(v.as_bytes());
            self.0.push(0);
            self
        }
        fn text(self, v: &str) -> Self {
            let mut w = self.u8(b't').i32(v.len() as i32);
            w.0.extend(v.as_bytes());
            w
        }
        fn done(self) -> Bytes {
            Bytes::from(self.0)
        }
    }

    fn text(v: &str) -> Datum {
        Datum::Text(Bytes::copy_from_slice(v.as_bytes()))
    }

    #[test]
    fn relation() {
        let m = W::default()
            .u8(b'R')
            .i32(16384)
            .s("public")
            .s("orders")
            .u8(b'd')
            .i16(2)
            .u8(1)
            .s("id")
            .i32(20)
            .i32(-1)
            .u8(0)
            .s("note")
            .i32(1043)
            .i32(104)
            .done();
        assert_eq!(
            decode(&m).unwrap(),
            Message::Relation {
                oid: 16384,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: b'd',
                columns: vec![
                    RelationColumn {
                        key: true,
                        name: "id".into(),
                        type_oid: 20,
                        typmod: -1
                    },
                    RelationColumn {
                        key: false,
                        name: "note".into(),
                        type_oid: 1043,
                        typmod: 104
                    },
                ],
            }
        );
    }

    #[test]
    fn type_and_origin() {
        let y = W::default()
            .u8(b'Y')
            .i32(70000)
            .s("public")
            .s("mood")
            .done();
        assert_eq!(
            decode(&y).unwrap(),
            Message::Type {
                oid: 70000,
                namespace: "public".into(),
                name: "mood".into()
            }
        );
        let o = W::default().u8(b'O').u64(0x1_0000_0010).s("node_a").done();
        assert_eq!(
            decode(&o).unwrap(),
            Message::Origin {
                commit_lsn: 0x1_0000_0010,
                name: "node_a".into()
            }
        );
    }

    #[test]
    fn insert_with_every_datum_kind() {
        let m = W::default()
            .u8(b'I')
            .i32(16384)
            .u8(b'N')
            .i16(4)
            .text("42")
            .u8(b'n')
            .u8(b'u')
            .text("")
            .done();
        assert_eq!(
            decode(&m).unwrap(),
            Message::Insert {
                relation: 16384,
                new: vec![text("42"), Datum::Null, Datum::Unchanged, text("")],
            }
        );
    }

    #[test]
    fn updates_with_and_without_an_old_tuple() {
        let plain = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'N')
            .i16(1)
            .text("a")
            .done();
        assert_eq!(
            decode(&plain).unwrap(),
            Message::Update {
                relation: 1,
                old: None,
                new: vec![text("a")]
            }
        );
        let key = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'K')
            .i16(2)
            .text("1")
            .u8(b'n')
            .u8(b'N')
            .i16(2)
            .text("2")
            .text("b")
            .done();
        assert_eq!(
            decode(&key).unwrap(),
            Message::Update {
                relation: 1,
                old: Some((OldKind::Key, vec![text("1"), Datum::Null])),
                new: vec![text("2"), text("b")],
            }
        );
        let full = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'O')
            .i16(1)
            .text("x")
            .u8(b'N')
            .i16(1)
            .text("y")
            .done();
        assert!(matches!(
            decode(&full).unwrap(),
            Message::Update {
                old: Some((OldKind::Full, _)),
                ..
            }
        ));
    }

    #[test]
    fn deletes_and_truncates() {
        let d = W::default()
            .u8(b'D')
            .i32(9)
            .u8(b'K')
            .i16(1)
            .text("7")
            .done();
        assert_eq!(
            decode(&d).unwrap(),
            Message::Delete {
                relation: 9,
                old: (OldKind::Key, vec![text("7")])
            }
        );
        let t = W::default().u8(b'T').i32(2).u8(3).i32(9).i32(10).done();
        assert_eq!(
            decode(&t).unwrap(),
            Message::Truncate {
                options: 3,
                relations: vec![9, 10]
            }
        );
    }

    #[test]
    fn malformed_messages_are_refused() {
        let cases = [
            (W::default().done(), "message tag"),
            (W::default().u8(b'Z').done(), "unexpected message tag 'Z'"),
            (W::default().u8(b'I').i32(1).u8(b'K').done(), "expected 'N'"),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b'b')
                    .i32(1)
                    .u8(1)
                    .done(),
                "binary",
            ),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b't')
                    .i32(5)
                    .done(),
                "text value: the message ends",
            ),
            (
                W::default().u8(b'I').i32(1).u8(b'N').i16(1).u8(b'x').done(),
                "unknown tuple column kind 'x'",
            ),
            (W::default().u8(b'Y').i32(1).u8(b'a').done(), "no NUL"),
            (
                W::default().u8(b'D').i32(1).u8(b'N').i16(0).done(),
                "expected 'K' or 'O'",
            ),
            (
                W::default().u8(b'T').i32(1).u8(0).i32(9).u8(0).done(),
                "1 bytes left over",
            ),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b't')
                    .i32(-1)
                    .done(),
                "text length -1",
            ),
        ];
        for (m, expect) in cases {
            let e = decode(&m).unwrap_err();
            assert!(e.contains(expect), "{e:?} should mention {expect:?}");
        }
    }

    proptest::proptest! {
        /// Arbitrary bytes never panic the decoder.
        #[test]
        fn arbitrary_payloads_never_panic(data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
            let _ = decode(&Bytes::from(data));
        }

        /// Counts near `i32::MAX` after a valid tag don't allocate for them.
        #[test]
        fn huge_counts_are_bounded_by_the_payload(tag in proptest::sample::select(vec![b'T', b'R', b'I'])) {
            let m = match tag {
                b'T' => W::default().u8(b'T').i32(i32::MAX).u8(0).done(),
                b'R' => W::default().u8(b'R').i32(1).s("s").s("t").u8(b'd').i16(i16::MAX).done(),
                _ => W::default().u8(b'I').i32(1).u8(b'N').i16(i16::MAX).done(),
            };
            proptest::prop_assert!(decode(&m).is_err());
        }
    }
}
