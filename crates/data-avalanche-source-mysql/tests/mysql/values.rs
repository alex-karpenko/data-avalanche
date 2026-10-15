//! The decoder against the server's own conversions, value by value: what the binlog carries
//! (raw bytes) decodes to what a chunk reads (the server's text).

use data_avalanche_core::ColumnValue;
use data_avalanche_source_mysql::{Charset, Decode, Flavour, Input, decode};
use data_avalanche_testkit::require_it;
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;

use crate::common::{admin_in, target};

fn from_binlog(d: &Decode, bytes: Vec<u8>) -> ColumnValue {
    decode(
        d,
        Input::Binlog(&BinlogValue::Value(Value::Bytes(bytes))),
        false,
    )
    .unwrap()
}

fn from_row(d: &Decode, v: &Value) -> ColumnValue {
    decode(d, Input::Row(v), false).unwrap()
}

/// Every latin1 byte: the binlog's raw byte decodes to the character the server converts it to.
#[tokio::test]
async fn latin1_bytes_decode_as_the_server_converts_them() {
    require_it!();
    let mut a = admin_in("avalanche").await;
    let d = Decode::Text {
        charset: Charset::Latin1,
    };
    for byte in 1..=255u8 {
        let v: Value = a
            .exec_first(
                "SELECT CONVERT(UNHEX(?) USING latin1)",
                (format!("{byte:02X}"),),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            from_binlog(&d, vec![byte]),
            from_row(&d, &v),
            "byte {byte:#04x}"
        );
    }
}

/// MariaDB `INET6` and `INET4`: the stored bytes, trailing zeros stripped as the binlog strips
/// them, print as MariaDB prints the value.
#[tokio::test]
async fn mariadb_addresses_print_as_mariadb_prints_them() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let mut a = admin_in("avalanche").await;
    for (d, kind, text) in [
        (Decode::Inet6, "INET6", "2001:db8::1"),
        (Decode::Inet6, "INET6", "::"),
        (Decode::Inet6, "INET6", "::1"),
        (Decode::Inet6, "INET6", "1::"),
        (Decode::Inet6, "INET6", "1:0:0:2::3"),
        (Decode::Inet6, "INET6", "1:0:0:2:0:0:0:3"),
        (Decode::Inet6, "INET6", "::ffff:192.168.0.1"),
        (Decode::Inet6, "INET6", "::192.168.0.1"),
        (Decode::Inet6, "INET6", "::2"),
        (Decode::Inet6, "INET6", "::0.0.1.0"),
        (Decode::Inet6, "INET6", "::1:0:0"),
        (Decode::Inet6, "INET6", "::1:2"),
        (Decode::Inet6, "INET6", "::ffff:0.0.0.1"),
        (Decode::Inet6, "INET6", "0:0:0:0:0:1:0:0"),
        (Decode::Inet6, "INET6", "1:0:2:0:3:0:4:0"),
        (Decode::Inet6, "INET6", "fe80::1:2:3:4"),
        (
            Decode::Inet6,
            "INET6",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        ),
        (Decode::Inet4, "INET4", "192.168.0.1"),
        (Decode::Inet4, "INET4", "0.0.0.0"),
        (Decode::Inet4, "INET4", "10.0.0.0"),
    ] {
        let (hex, printed): (String, Value) = a
            .exec_first(
                format!("SELECT HEX(CAST(? AS {kind})), CAST(? AS {kind})"),
                (text, text),
            )
            .await
            .unwrap()
            .unwrap();
        let mut bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        assert_eq!(
            from_binlog(&d, bytes),
            from_row(&d, &printed),
            "{kind} {text}"
        );
    }
}

/// 300 sparse addresses (each group zero with probability ½, so runs of every length and place)
/// print as MariaDB prints them.
#[tokio::test]
async fn sparse_mariadb_inet6_addresses_print_as_mariadb_prints_them() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let mut a = admin_in("avalanche").await;
    let mut seed: u64 = 0x5eed;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as u16
    };
    for _ in 0..300 {
        let words: Vec<u16> = (0..8)
            .map(|_| if next() % 2 == 0 { 0 } else { next() })
            .collect();
        let hex: String = words.iter().map(|w| format!("{w:04X}")).collect();
        let printed: Value = a
            .exec_first("SELECT CAST(UNHEX(?) AS INET6)", (hex.clone(),))
            .await
            .unwrap()
            .unwrap();
        let mut bytes: Vec<u8> = words.iter().flat_map(|w| w.to_be_bytes()).collect();
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        assert_eq!(
            from_binlog(&Decode::Inet6, bytes),
            from_row(&Decode::Inet6, &printed),
            "{hex}"
        );
    }
}
