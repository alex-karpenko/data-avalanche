//! The pipeline start checks (config §11.2, the MySQL rows) against the target server: its
//! settings pass, and a user without the replication privileges is refused, naming the one it
//! lacks. The test servers aren't replicas and filter no database: MySQL logs replica updates,
//! MariaDB doesn't (its default), and a MariaDB user without SLAVE MONITOR can't read whether the
//! server replicates, which warns.

use data_avalanche_core::SourceError;
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{Names, cleanup, connector, fresh, pipeline, pipeline_as, target, user};

#[tokio::test]
async fn the_test_server_passes_preflight() {
    require_it!();
    let (n, _a) = fresh("preflight_ok", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let p = c.preflight().await.expect("preflight");
    assert_eq!(c.flavour().await.unwrap(), target().flavour);
    assert_eq!(p.server_version, c.server_version().await.unwrap());
    assert_eq!(p.warnings, Vec::<String>::new());
    cleanup(&n).await;
}

/// MySQL without GTIDs refuses `replication_type: gtid` (file/position mode reads it).
#[tokio::test]
async fn gtid_replication_needs_gtid_mode_on() {
    require_it!();
    if target().name != "mysql-84-nogtid" {
        eprintln!("skipped: needs the mysql-84-nogtid target");
        return;
    }
    let (n, _a) = fresh("preflight_gtid_off", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ", replication_type: gtid"));
    match c.preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("gtid_mode is OFF"), "{m}"),
        other => panic!("refused expected, got {other:?}"),
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_user_without_replication_privileges_is_refused() {
    require_it!();
    let (n, mut a) = fresh("preflight_grants", "").await;
    let name = "it_preflight_grants";
    let status_privilege = match target().flavour {
        Flavour::Mysql => "REPLICATION CLIENT",
        Flavour::Mariadb => "BINLOG MONITOR",
    };
    let mariadb = target().flavour == Flavour::Mariadb;
    let mut cases = vec![
        (format!("SELECT ON {}.* ", n.db), Some(status_privilege), 0),
        (
            "SELECT, REPLICATION CLIENT ON *.*".to_string(),
            Some("REPLICATION SLAVE"),
            0,
        ),
        (
            "SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.*".to_string(),
            None,
            usize::from(mariadb),
        ),
    ];
    if mariadb {
        cases.push((
            "SELECT, REPLICATION CLIENT, REPLICATION SLAVE, SLAVE MONITOR ON *.*".to_string(),
            None,
            0,
        ));
    }
    for (grants, refusal, warned) in cases {
        user(&mut a, name, &grants, &[]).await;
        let c = connector(&pipeline_as(
            &n,
            name,
            "      ssl_mode: disable",
            "",
            &["t"],
            "",
        ));
        match (c.preflight().await, refusal) {
            (Ok(p), None) => {
                assert_eq!(p.warnings.len(), warned, "{grants}: {:?}", p.warnings);
                assert!(
                    p.warnings
                        .iter()
                        .all(|w| w.contains("log_slave_updates is OFF")
                            && w.contains("grant SLAVE MONITOR")),
                    "{grants}: {:?}",
                    p.warnings
                );
            }
            (Err(SourceError::Permanent(m)), Some(lacking)) => {
                assert!(m.contains(lacking), "{grants}: {m}")
            }
            (other, _) => panic!("{grants}: {other:?}"),
        }
    }
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    cleanup(&n).await;
}

/// The privileges a role grants count when the role is active at login (a default role).
#[tokio::test]
async fn replication_granted_through_a_default_role_passes() {
    require_it!();
    let (n, mut a) = fresh("preflight_role", "").await;
    let (name, role) = ("it_preflight_role", "it_preflight_role_r");
    a.query_drop(format!("DROP ROLE IF EXISTS {role}"))
        .await
        .unwrap();
    a.query_drop(format!("CREATE ROLE {role}")).await.unwrap();
    a.query_drop(format!(
        "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO {role}"
    ))
    .await
    .unwrap();
    let default_role = match target().flavour {
        Flavour::Mysql => format!("SET DEFAULT ROLE {role} TO '{name}'@'%'"),
        Flavour::Mariadb => format!("SET DEFAULT ROLE {role} FOR '{name}'@'%'"),
    };
    user(&mut a, name, role, &[default_role]).await;
    let c = connector(&pipeline_as(
        &n,
        name,
        "      ssl_mode: disable",
        "",
        &["t"],
        "",
    ));
    c.preflight().await.expect("the role's privileges count");
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    a.query_drop(format!("DROP ROLE {role}")).await.unwrap();
    cleanup(&n).await;
}

/// `ssl_mode: prefer` connects without TLS to a server that offers none, and `require` is refused
/// there (`mysql-84-nogtid` runs with TLS off).
#[tokio::test]
async fn prefer_falls_back_to_plain_and_require_is_refused() {
    require_it!();
    if target().name != "mysql-84-nogtid" {
        eprintln!("skipped: needs the mysql-84-nogtid target (TLS off)");
        return;
    }
    let (n, _a) = fresh("preflight_prefer", "").await;
    let source = |mode: &str| format!("      ssl_mode: {mode}");
    let c = connector(&pipeline_as(&n, "root", &source("prefer"), "", &["t"], ""));
    c.preflight().await.expect("prefer connects without TLS");
    let c = connector(&pipeline_as(&n, "root", &source("require"), "", &["t"], ""));
    match c.preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("doesn't offer TLS"), "{m}"),
        other => panic!("refused expected, got {other:?}"),
    }
    cleanup(&n).await;
}

#[test]
fn names_are_per_test() {
    assert_eq!(Names::new("x").db, "it_x");
}
