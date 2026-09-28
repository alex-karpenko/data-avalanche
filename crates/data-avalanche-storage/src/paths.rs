//! Storage locations (config §10). Segments are joined with `/`; empty segments are skipped and
//! leading or trailing slashes in a segment don't add separators, so a `prefix` of `""` is the
//! bucket root.

/// A bucket (Azure: container) and the key prefix under it; `root` has no leading or trailing
/// `/`, and `""` is the bucket root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub bucket: String,
    pub root: String,
}

/// The marker segment that separates co-located pipeline state from table data (config §10).
pub const COLOCATED_STATE_DIR: &str = ".pipeline-state";

/// Joins segments with one `/`, skipping empty ones: `["/a/", "", "b//c"]` → `"a/b/c"`.
pub fn join(segments: &[&str]) -> String {
    segments
        .iter()
        .flat_map(|s| s.split('/'))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// Makes a database identifier safe as one key segment: `%`, `/` and control characters become
/// `%XX`, and a name of `.` or `..` has its dots escaped. Everything else (including `.` inside a
/// name and non-ASCII text) is kept, so ordinary names map to themselves.
pub fn escape_segment(ident: &str) -> String {
    if ident == "." || ident == ".." {
        return ident.replace('.', "%2E");
    }
    let mut out = String::with_capacity(ident.len());
    for c in ident.chars() {
        if c == '%' || c == '/' || c.is_control() {
            let mut buf = [0u8; 4];
            for byte in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{byte:02X}"));
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: one table's root, for every sink format. For
/// MySQL, `schema` is the database (`source.db`).
pub fn table_root(sink_bucket: &str, sink_prefix: &str, schema: &str, table: &str) -> Location {
    let (schema, table) = (escape_segment(schema), escape_segment(table));
    Location {
        bucket: sink_bucket.to_owned(),
        root: join(&[sink_prefix, &schema, &table]),
    }
}

/// `<sink.bucket>/<sink.prefix>/.pipeline-state/`: pipeline state stored with the sink data.
/// `defaults.state.prefix` doesn't apply here.
pub fn colocated_state(sink_bucket: &str, sink_prefix: &str) -> Location {
    Location {
        bucket: sink_bucket.to_owned(),
        root: join(&[sink_prefix, COLOCATED_STATE_DIR]),
    }
}

/// `<state.bucket>/[<defaults.state.prefix>/]<state.prefix>/`: an explicit pipeline state
/// location. `parent_prefix` is `defaults.state.prefix`.
pub fn explicit_state(bucket: &str, parent_prefix: Option<&str>, prefix: &str) -> Location {
    Location {
        bucket: bucket.to_owned(),
        root: join(&[parent_prefix.unwrap_or(""), prefix]),
    }
}

/// `<globals.state.bucket>/<globals.state.prefix>/`: cluster-wide state.
pub fn globals_state(bucket: &str, prefix: &str) -> Location {
    Location {
        bucket: bucket.to_owned(),
        root: join(&[prefix]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(bucket: &str, root: &str) -> Location {
        Location {
            bucket: bucket.into(),
            root: root.into(),
        }
    }

    #[test]
    fn join_skips_empty_segments_and_extra_slashes() {
        assert_eq!(join(&["/a/", "", "b//c", "/"]), "a/b/c");
        assert_eq!(join(&["", ""]), "");
        assert_eq!(join(&[]), "");
    }

    #[test]
    fn table_root_under_sink_prefix() {
        assert_eq!(
            table_root("lake", "orders-pg", "public", "orders"),
            loc("lake", "orders-pg/public/orders")
        );
        assert_eq!(
            table_root("lake", "/team/cdc/", "shop", "items"),
            loc("lake", "team/cdc/shop/items")
        );
        assert_eq!(
            table_root("lake", "", "public", "orders"),
            loc("lake", "public/orders"),
            "prefix \"\" is the bucket root"
        );
    }

    #[test]
    fn table_root_escapes_only_what_breaks_a_segment() {
        assert_eq!(
            table_root("b", "p", "sales", "order.items").root,
            "p/sales/order.items"
        );
        assert_eq!(
            table_root("b", "p", "продажи", "Заказы 2026").root,
            "p/продажи/Заказы 2026"
        );
        assert_eq!(table_root("b", "p", "a/b", "50%").root, "p/a%2Fb/50%25");
        assert_eq!(table_root("b", "p", "..", ".").root, "p/%2E%2E/%2E");
        assert_eq!(table_root("b", "p", "s", "t\n").root, "p/s/t%0A");
    }

    #[test]
    fn escaped_roots_are_valid_store_roots() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        for (schema, table) in [("..", "."), ("a/b", "c\u{0}d"), ("s", "%2F"), ("", "")] {
            let root = table_root("b", "p", schema, table).root;
            crate::ObjectStoreVersioned::new(mem.clone(), &root)
                .unwrap_or_else(|e| panic!("{root:?}: {e}"));
        }
    }

    #[test]
    fn colocated_state_is_marked_under_sink_prefix() {
        assert_eq!(
            colocated_state("lake", "orders-pg"),
            loc("lake", "orders-pg/.pipeline-state")
        );
        assert_eq!(colocated_state("lake", ""), loc("lake", ".pipeline-state"));
    }

    #[test]
    fn explicit_state_composes_parent_prefix() {
        assert_eq!(
            explicit_state("ops", Some("cdc-state"), "orders-pg"),
            loc("ops", "cdc-state/orders-pg")
        );
        assert_eq!(
            explicit_state("ops", None, "orders-pg"),
            loc("ops", "orders-pg")
        );
        assert_eq!(
            explicit_state("ops", Some(""), "/orders-pg/"),
            loc("ops", "orders-pg")
        );
        assert_eq!(
            explicit_state("ops", Some("cdc-state"), ""),
            loc("ops", "cdc-state")
        );
    }

    #[test]
    fn globals_state_prefix() {
        assert_eq!(globals_state("ops", "cluster-a"), loc("ops", "cluster-a"));
        assert_eq!(globals_state("ops", ""), loc("ops", ""));
        assert_eq!(globals_state("ops", "/a//b/"), loc("ops", "a/b"));
    }
}
