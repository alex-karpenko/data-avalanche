use data_avalanche_testkit::{endpoints, install_crypto_provider, require_it};
use mysql_async::prelude::Queryable;

#[tokio::test]
async fn postgres_18_runs_with_logical_wal() {
    require_it!();
    let (client, conn) = tokio_postgres::connect(endpoints::POSTGRES_18, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let row = client.query_one("SHOW wal_level", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "logical");
}

#[tokio::test]
async fn mysql_84_has_gtid_and_full_row_image() {
    require_it!();
    install_crypto_provider();
    let pool = mysql_async::Pool::new(endpoints::MYSQL_84);
    let mut conn = pool.get_conn().await.unwrap();
    let (gtid, format, image): (String, String, String) = conn
        .query_first("SELECT @@gtid_mode, @@binlog_format, @@binlog_row_image")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (gtid.as_str(), format.as_str(), image.as_str()),
        ("ON", "ROW", "FULL")
    );
    drop(conn);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn mariadb_118_has_row_format_and_strict_gtid() {
    require_it!();
    install_crypto_provider();
    let pool = mysql_async::Pool::new(endpoints::MARIADB_118);
    let mut conn = pool.get_conn().await.unwrap();
    let (format, image, strict): (String, String, i64) = conn
        .query_first("SELECT @@binlog_format, @@binlog_row_image, @@gtid_strict_mode")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (format.as_str(), image.as_str(), strict),
        ("ROW", "FULL", 1)
    );
    drop(conn);
    pool.disconnect().await.unwrap();
}
