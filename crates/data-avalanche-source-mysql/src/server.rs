//! The server behind the source: MySQL or MariaDB, its version, and the start checks on its
//! settings and the user's privileges (config §11.2, the MySQL rows; P-8 versions). The checks
//! are pure functions over what the server reported, so every refusal is unit-tested.

use data_avalanche_config::ReplicationType;
use data_avalanche_core::TableRef;

/// The binlog dialect: GTIDs, status statements, row events and types differ (S-MY10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavour {
    Mysql,
    Mariadb,
}

impl Flavour {
    /// From `VERSION()`: MariaDB's carries `-MariaDB` (S-MY10a).
    pub fn of(version: &str) -> Self {
        if version.contains("MariaDB") {
            Self::Mariadb
        } else {
            Self::Mysql
        }
    }

    /// The oldest supported release (P-8): MySQL 8.4 LTS, MariaDB 10.11 LTS.
    pub fn minimum(self) -> (u16, u16) {
        match self {
            Self::Mysql => (8, 4),
            Self::Mariadb => (10, 11),
        }
    }

    /// The statement that reads the binlog write position: MySQL 8.4 accepts only `SHOW BINARY
    /// LOG STATUS`, MariaDB 10.11 and 11.4 only `SHOW BINLOG STATUS` (S-MY10a).
    pub fn status_statement(self) -> &'static str {
        match self {
            Self::Mysql => "SHOW BINARY LOG STATUS",
            Self::Mariadb => "SHOW BINLOG STATUS",
        }
    }

    /// The privilege that statement needs, as the server names it.
    pub fn status_privilege(self) -> &'static str {
        match self {
            Self::Mysql => "REPLICATION CLIENT",
            Self::Mariadb => "BINLOG MONITOR (or REPLICATION CLIENT)",
        }
    }

    /// The setting that has a replica log what it applies: MariaDB 10.11–11.8 has only the old
    /// name (measured).
    pub fn replica_updates(self) -> &'static str {
        match self {
            Self::Mysql => "log_replica_updates",
            Self::Mariadb => "log_slave_updates",
        }
    }

    /// The statement that lists the servers this one replicates from, one row each.
    pub fn replica_statement(self) -> &'static str {
        match self {
            Self::Mysql => "SHOW REPLICA STATUS",
            Self::Mariadb => "SHOW ALL REPLICAS STATUS",
        }
    }

    /// The privilege that statement needs, as the server names it (measured: MySQL 8.4 takes
    /// REPLICATION CLIENT, MariaDB 10.11–11.8 SLAVE MONITOR).
    pub fn replica_privilege(self) -> &'static str {
        match self {
            Self::Mysql => "REPLICATION CLIENT",
            Self::Mariadb => "SLAVE MONITOR (REPLICA MONITOR)",
        }
    }
}

/// How the server compares table and database names (`lower_case_table_names`): as written
/// under 0, without case under 1 and 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Fold {
    pub fold: bool,
}

impl Fold {
    /// From `@@lower_case_table_names`.
    pub fn of(lower_case_table_names: i64) -> Self {
        Self {
            fold: lower_case_table_names != 0,
        }
    }

    /// Whether `a` and `b` name one table or database.
    pub fn eq(self, a: &str, b: &str) -> bool {
        match self.fold {
            true => a == b || a.to_lowercase() == b.to_lowercase(),
            false => a == b,
        }
    }

    /// Whether `a` and `b` are one table.
    pub fn same(self, a: &TableRef, b: &TableRef) -> bool {
        self.eq(&a.schema, &b.schema) && self.eq(&a.name, &b.name)
    }

    /// `t` as the server compares it: lower case when folding.
    pub fn key(self, t: &TableRef) -> TableRef {
        match self.fold {
            true => TableRef::new(t.schema.to_lowercase(), t.name.to_lowercase()),
            false => t.clone(),
        }
    }
}

impl std::fmt::Display for Flavour {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Mysql => "MySQL",
            Self::Mariadb => "MariaDB",
        })
    }
}

/// `major.minor` from a `VERSION()` string (`8.4.6`, `11.8.3-MariaDB-ubu2404-log`).
pub fn version_of(version: &str) -> Option<(u16, u16)> {
    let mut parts = version.split(['.', '-']);
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// The server settings the start checks read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// `VERSION()`.
    pub version: String,
    pub log_bin: bool,
    pub binlog_format: String,
    pub binlog_row_image: String,
    /// `None` on a server without the variable.
    pub binlog_row_metadata: Option<String>,
    /// MySQL `@@gtid_mode`; `None` on MariaDB, whose GTIDs are always on.
    pub gtid_mode: Option<String>,
}

/// The pipeline-level checks on `settings` (config §11.2): `Err` refuses the pipeline, the `Ok`
/// warnings let it start.
pub fn check_settings(
    flavour: Flavour,
    replication: ReplicationType,
    s: &Settings,
) -> Result<Vec<String>, String> {
    let (major, minor) = version_of(&s.version)
        .ok_or_else(|| format!("can't read the server version from {:?}", s.version))?;
    let (min_major, min_minor) = flavour.minimum();
    if (major, minor) < (min_major, min_minor) {
        return Err(format!(
            "{flavour} {}: the source needs {flavour} {min_major}.{min_minor} or newer",
            s.version
        ));
    }
    if !s.log_bin {
        return Err("the binary log is off (log_bin); the source reads the binlog".into());
    }
    if !s.binlog_format.eq_ignore_ascii_case("ROW") {
        return Err(format!(
            "binlog_format is {}; the source needs binlog_format = ROW",
            s.binlog_format
        ));
    }
    if !s.binlog_row_image.eq_ignore_ascii_case("FULL") {
        return Err(format!(
            "binlog_row_image is {}; the source needs binlog_row_image = FULL",
            s.binlog_row_image
        ));
    }
    if let (ReplicationType::Gtid, Some(mode)) = (replication, &s.gtid_mode)
        && !mode.eq_ignore_ascii_case("ON")
    {
        return Err(format!(
            "gtid_mode is {mode}; replication_type: gtid needs gtid_mode = ON (or use replication_type: binlog)"
        ));
    }
    match &s.binlog_row_metadata {
        Some(m) if m.eq_ignore_ascii_case("FULL") => Ok(vec![]),
        other => Err(format!(
            "binlog_row_metadata is {}; the source needs binlog_row_metadata = FULL (SET GLOBAL \
             binlog_row_metadata = FULL)",
            other.as_deref().unwrap_or("not available")
        )),
    }
}

/// What the server reports about which changes reach its binlog (config §11.2).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Coverage {
    /// The status statement's `Binlog_Do_DB` and `Binlog_Ignore_DB`: comma-separated, empty for
    /// none.
    pub(crate) do_db: String,
    pub(crate) ignore_db: String,
    /// `log_replica_updates` (MariaDB `log_slave_updates`) is on.
    pub(crate) replica_updates: bool,
    /// Whether the server replicates from another; `None` when the user can't read it.
    pub(crate) replica: Option<bool>,
}

/// Whether `db`'s changes reach the binlog (config §11.2): `Err` refuses the pipeline when a
/// binlog filter leaves `db` out, or when the server replicates from another without logging
/// what it applies; `Ok(Some)` warns when the user can't read whether it replicates.
pub(crate) fn check_coverage(
    flavour: Flavour,
    db: &str,
    fold: Fold,
    c: &Coverage,
) -> Result<Option<String>, String> {
    let list = |s: &str| -> Vec<String> {
        s.split(',')
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .map(str::to_owned)
            .collect()
    };
    let (only, ignored) = (list(&c.do_db), list(&c.ignore_db));
    if !only.is_empty() && !only.iter().any(|d| fold.eq(d, db)) {
        return Err(format!(
            "binlog_do_db is {}, without {db}: the binlog doesn't carry the source database's \
             changes",
            c.do_db
        ));
    }
    if ignored.iter().any(|d| fold.eq(d, db)) {
        return Err(format!(
            "binlog_ignore_db lists {db}: the binlog doesn't carry the source database's changes"
        ));
    }
    let setting = flavour.replica_updates();
    match (c.replica_updates, c.replica) {
        (true, _) | (false, Some(false)) => Ok(None),
        (false, Some(true)) => Err(format!(
            "this server replicates from another and doesn't log what it applies ({setting} = \
             OFF): the binlog lacks the replicated changes; set {setting} = ON"
        )),
        (false, None) => Ok(Some(format!(
            "{setting} is OFF, and the user can't read whether this server replicates from \
             another ({}; grant {}): if it does, the binlog lacks the replicated changes",
            flavour.replica_statement(),
            flavour.replica_privilege()
        ))),
    }
}

/// True when `SHOW GRANTS` lines give `REPLICATION SLAVE` on `*.*` (directly or through `ALL
/// PRIVILEGES`). MariaDB 10.5+ still prints the privilege under that name; its separate
/// `REPLICATION SLAVE ADMIN` doesn't allow a binlog dump, so names are compared whole.
pub fn grants_replication(grants: &[String]) -> bool {
    grants.iter().any(|g| {
        let g = g.to_ascii_uppercase();
        let Some((privileges, on)) = g
            .strip_prefix("GRANT ")
            .and_then(|rest| rest.split_once(" ON "))
        else {
            return false;
        };
        on.starts_with("*.* ")
            && privileges
                .split(',')
                .map(str::trim)
                .any(|p| p == "ALL PRIVILEGES" || p == "ALL" || p == "REPLICATION SLAVE")
    })
}

/// True for a server error that refuses a statement for want of a privilege:
/// `ER_DBACCESS_DENIED_ERROR` (1044), `ER_TABLEACCESS_DENIED_ERROR` (1142),
/// `ER_SPECIFIC_ACCESS_DENIED_ERROR` (1227).
pub(crate) fn access_denied(code: u16) -> bool {
    matches!(code, 1044 | 1142 | 1227)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(version: &str) -> Settings {
        Settings {
            version: version.into(),
            log_bin: true,
            binlog_format: "ROW".into(),
            binlog_row_image: "FULL".into(),
            binlog_row_metadata: Some("FULL".into()),
            gtid_mode: Flavour::of(version)
                .eq(&Flavour::Mysql)
                .then(|| "ON".into()),
        }
    }

    #[test]
    fn the_flavour_and_version_come_from_the_version_string() {
        assert_eq!(Flavour::of("8.4.6"), Flavour::Mysql);
        assert_eq!(Flavour::of("11.8.3-MariaDB-ubu2404-log"), Flavour::Mariadb);
        assert_eq!(version_of("8.4.6"), Some((8, 4)));
        assert_eq!(version_of("10.11.14-MariaDB-log"), Some((10, 11)));
        assert_eq!(version_of("8"), None);
    }

    #[test]
    fn supported_servers_with_the_settings_pass() {
        for (v, f) in [
            ("8.4.6", Flavour::Mysql),
            ("9.4.0", Flavour::Mysql),
            ("10.11.14-MariaDB", Flavour::Mariadb),
            ("11.8.3-MariaDB-log", Flavour::Mariadb),
        ] {
            for r in [ReplicationType::Gtid, ReplicationType::Binlog] {
                assert_eq!(check_settings(f, r, &settings(v)), Ok(vec![]), "{v}");
            }
        }
    }

    #[test]
    fn old_servers_and_wrong_settings_refuse_the_pipeline() {
        let refused = |f, r, s: Settings, needle: &str| {
            let e = check_settings(f, r, &s).unwrap_err();
            assert!(e.contains(needle), "{e}");
        };
        let (my, maria, gtid, binlog) = (
            Flavour::Mysql,
            Flavour::Mariadb,
            ReplicationType::Gtid,
            ReplicationType::Binlog,
        );
        refused(my, gtid, settings("8.0.39"), "needs MySQL 8.4 or newer");
        refused(
            maria,
            gtid,
            settings("10.6.21-MariaDB"),
            "needs MariaDB 10.11",
        );
        let mut s = settings("8.4.6");
        s.log_bin = false;
        refused(my, binlog, s, "log_bin");
        let mut s = settings("8.4.6");
        s.binlog_format = "MIXED".into();
        refused(my, binlog, s, "binlog_format = ROW");
        let mut s = settings("11.8.3-MariaDB");
        s.binlog_row_image = "MINIMAL".into();
        refused(maria, gtid, s, "binlog_row_image = FULL");
        let mut s = settings("8.4.6");
        s.gtid_mode = Some("ON_PERMISSIVE".into());
        refused(my, gtid, s.clone(), "gtid_mode = ON");
        // File/position mode reads a server without GTIDs.
        assert_eq!(check_settings(my, binlog, &s), Ok(vec![]));
    }

    #[test]
    fn row_metadata_other_than_full_refuses_the_pipeline() {
        for (flavour, version, m) in [
            (Flavour::Mysql, "8.4.6", Some("MINIMAL")),
            (Flavour::Mariadb, "11.8.3-MariaDB", Some("NO_LOG")),
            (Flavour::Mysql, "8.4.6", None),
        ] {
            let mut s = settings(version);
            s.binlog_row_metadata = m.map(str::to_owned);
            let e = check_settings(flavour, ReplicationType::Gtid, &s).unwrap_err();
            assert!(
                e.starts_with(&format!(
                    "binlog_row_metadata is {}; the source needs binlog_row_metadata = FULL",
                    m.unwrap_or("not available")
                )) && e.contains("SET GLOBAL binlog_row_metadata = FULL"),
                "{e}"
            );
        }
    }

    #[test]
    fn the_binlog_must_carry_the_source_database() {
        let (my, maria) = (Flavour::Mysql, Flavour::Mariadb);
        let (exact, folded) = (Fold::of(0), Fold::of(1));
        let cover = |do_db: &str, ignore_db: &str, updates: bool, replica: Option<bool>| Coverage {
            do_db: do_db.into(),
            ignore_db: ignore_db.into(),
            replica_updates: updates,
            replica,
        };
        let ok = cover("", "", true, None);
        assert_eq!(check_coverage(my, "shop", exact, &ok), Ok(None));
        // Binlog filters.
        assert_eq!(
            check_coverage(my, "shop", exact, &cover("a, shop", "b", true, None)),
            Ok(None)
        );
        let e = check_coverage(my, "shop", exact, &cover("a,b", "", true, None)).unwrap_err();
        assert!(e.starts_with("binlog_do_db is a,b, without shop"), "{e}");
        let e = check_coverage(my, "shop", exact, &cover("", "x,shop", true, None)).unwrap_err();
        assert!(e.starts_with("binlog_ignore_db lists shop"), "{e}");
        // Names fold under lower_case_table_names, and not otherwise.
        assert!(check_coverage(my, "shop", exact, &cover("Shop", "", true, None)).is_err());
        assert_eq!(
            check_coverage(my, "shop", folded, &cover("Shop", "", true, None)),
            Ok(None)
        );
        assert_eq!(
            check_coverage(my, "shop", exact, &cover("", "SHOP", true, None)),
            Ok(None)
        );
        assert!(check_coverage(my, "shop", folded, &cover("", "SHOP", true, None)).is_err());
        // A replica that doesn't log what it applies.
        assert_eq!(
            check_coverage(maria, "shop", exact, &cover("", "", false, Some(false))),
            Ok(None)
        );
        let e =
            check_coverage(maria, "shop", exact, &cover("", "", false, Some(true))).unwrap_err();
        assert!(
            e.contains("replicates from another") && e.contains("log_slave_updates = OFF"),
            "{e}"
        );
        let e = check_coverage(my, "shop", exact, &cover("", "", false, Some(true))).unwrap_err();
        assert!(e.contains("log_replica_updates = OFF"), "{e}");
        assert_eq!(
            check_coverage(my, "shop", exact, &cover("", "", true, Some(true))),
            Ok(None)
        );
        // Unreadable: a warning naming the grant.
        let w = check_coverage(maria, "shop", exact, &cover("", "", false, None))
            .unwrap()
            .unwrap();
        assert!(
            w.contains("SHOW ALL REPLICAS STATUS") && w.contains("grant SLAVE MONITOR"),
            "{w}"
        );
    }

    #[test]
    fn names_fold_under_lower_case_table_names() {
        let (exact, folded) = (Fold::of(0), Fold::of(2));
        let (a, b) = (
            TableRef::new("Shop", "Orders"),
            TableRef::new("shop", "orders"),
        );
        assert!(!exact.same(&a, &b) && folded.same(&a, &b));
        assert_eq!(folded.key(&a), b);
        assert_eq!(exact.key(&a), a);
        assert!(folded.eq("ÄB", "äb"));
    }

    #[test]
    fn replication_comes_from_a_global_grant() {
        let g = |lines: &[&str]| {
            grants_replication(&lines.iter().map(|l| l.to_string()).collect::<Vec<_>>())
        };
        assert!(g(&[
            "GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"
        ]));
        assert!(g(&[
            "GRANT USAGE ON *.* TO `u`@`%`",
            "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`",
        ]));
        assert!(!g(&["GRANT SELECT, REPLICATION CLIENT ON *.* TO `u`@`%`"]));
        // MariaDB's REPLICATION SLAVE ADMIN is another privilege.
        assert!(!g(&["GRANT REPLICATION SLAVE ADMIN ON *.* TO `u`@`%`"]));
        assert!(g(&[
            "GRANT REPLICATION SLAVE ADMIN, REPLICATION SLAVE ON *.* TO `u`@`%`"
        ]));
        // A database grant can't hold it.
        assert!(!g(&["GRANT ALL PRIVILEGES ON `shop`.* TO `u`@`%`"]));
    }

    #[test]
    fn only_a_privilege_error_refuses_the_status_statement() {
        assert!(access_denied(1227));
        assert!(access_denied(1044));
        assert!(access_denied(1142));
        // A shutdown, a lock wait or a syntax error isn't a missing grant.
        assert!(!access_denied(1053));
        assert!(!access_denied(1205));
        assert!(!access_denied(1064));
    }
}
