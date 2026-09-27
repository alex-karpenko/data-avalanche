//! The config's enumerations. Each enum's value-domain rule (V-nnn) reports a value outside the
//! enum; the reserved values that parse but are rejected in v1 (e.g. `lock_backend: kubernetes`)
//! are separate checks in `rules`.

use std::fmt;
use std::str::FromStr;

use serde::{Serialize, Serializer};

use crate::coerce::LeafType;

macro_rules! config_enum {
    ($(#[$meta:meta])* $name:ident, rule = $rule:literal, { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text),+
                }
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, String> {
                match s {
                    $($text => Ok($name::$variant),)+
                    _ => Err(format!(
                        "{s:?} is not one of: {}",
                        [$($text),+].join(", ")
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl LeafType for $name {
            const RULE: &'static str = $rule;

            fn parse_leaf(text: &str) -> Result<Self, String> {
                text.parse()
            }
        }
    };
}

config_enum!(Cloud, rule = "V-015", { Aws => "aws", Google => "google", Azure => "azure" });
config_enum!(ApiAuth, rule = "V-016", { None => "none", Basic => "basic", Jwt => "jwt", Oauth2 => "oauth2" });
config_enum!(LogFormat, rule = "V-018", { Json => "json", Compact => "compact", Pretty => "pretty" });
config_enum!(
    /// `kubernetes` is reserved: it parses, and V-075 rejects it in v1.
    LockBackend, rule = "V-075", { Storage => "storage", Kubernetes => "kubernetes" }
);
config_enum!(Discovery, rule = "V-095", { Static => "static", Dns => "dns" });
config_enum!(SourceType, rule = "V-019", { Postgres => "postgres", Mysql => "mysql" });
config_enum!(SslMode, rule = "V-020", {
    Disable => "disable", Allow => "allow", Prefer => "prefer", Require => "require",
    VerifyCa => "verify-ca", VerifyFull => "verify-full",
});
config_enum!(ToastStrategy, rule = "V-080", { CarryForward => "carry_forward", ReQuery => "re_query" });
config_enum!(SlotManagement, rule = "V-102", { Managed => "managed", RequireExisting => "require_existing" });
config_enum!(SlotOnOrphan, rule = "V-102", { Halt => "halt", AdoptResnapshot => "adopt_resnapshot" });
config_enum!(ReplicationType, rule = "V-021", { Binlog => "binlog", Gtid => "gtid" });
config_enum!(Tinyint1Detection, rule = "V-081", { InformationSchema => "information_schema", None => "none" });
config_enum!(BinlogPurgeAction, rule = "V-105", { Error => "error", ReSnapshot => "re_snapshot" });
config_enum!(SnapshotMode, rule = "V-022", {
    Never => "never", Initial => "initial", Always => "always",
    DumpAlways => "dump_always", DumpInitial => "dump_initial",
});
config_enum!(NumericOverflowAction, rule = "V-082", { String => "string", Error => "error" });
config_enum!(Transform, rule = "V-107", {
    Identity => "identity", Bucket => "bucket", Truncate => "truncate",
    Year => "year", Month => "month", Day => "day", Hour => "hour",
});
config_enum!(
    /// Both values are valid config; v1 refuses an `iceberg` pipeline at start (D-V1.4), not here.
    SinkType, rule = "V-083", { Iceberg => "iceberg", Delta => "delta" }
);
config_enum!(
    /// `changelog` is reserved: V-089 rejects it in v1.
    WriteMode, rule = "V-089", { Mirror => "mirror", Changelog => "changelog" }
);
config_enum!(IcebergStrategy, rule = "V-023", { Mor => "mor", Cow => "cow" });
config_enum!(
    /// `sink.iceberg.catalog.type`, the optional discriminator; V-002 also checks it against the blocks.
    CatalogKind, rule = "V-002", { Glue => "glue", Hive => "hive", Rest => "rest" }
);
config_enum!(HiveAuth, rule = "V-025", { None => "none", Ldap => "ldap", Kerberos => "kerberos" });
config_enum!(RestAuth, rule = "V-025", { None => "none", Bearer => "bearer", Oauth2 => "oauth2", Sigv4 => "sigv4" });
config_enum!(
    /// `name` is reserved: V-092 rejects it in v1.
    ColumnMapping, rule = "V-092", { None => "none", Name => "name" }
);
config_enum!(TimestampNtzAs, rule = "V-114", {
    TimestampNtz => "timestamp_ntz", Timestamp => "timestamp", String => "string",
});
config_enum!(
    /// Only `none` is implemented in v1 (V-088).
    DeltaCatalogType, rule = "V-088", { None => "none", Glue => "glue", Unity => "unity", Hive => "hive" }
);
config_enum!(
    /// Local API roles, `viewer ⊂ operator ⊂ admin` (`globals.api.role_mapping` keys, V-099).
    Role, rule = "V-099", { Viewer => "viewer", Operator => "operator", Admin => "admin" }
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_value_and_names_the_choices() {
        for m in SnapshotMode::ALL {
            assert_eq!(m.as_str().parse::<SnapshotMode>().unwrap(), *m);
        }
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
        let err = "gcp".parse::<Cloud>().unwrap_err();
        assert_eq!(err, "\"gcp\" is not one of: aws, google, azure");
        assert!("Aws".parse::<Cloud>().is_err(), "values are case-sensitive");
        assert_eq!(<Cloud as LeafType>::RULE, "V-015");
        assert_eq!(serde_json::to_value(RestAuth::Sigv4).unwrap(), "sigv4");
    }
}
