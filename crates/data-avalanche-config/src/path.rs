use std::fmt;

/// A location in the config document as a key path, e.g. `pipelines.orders.sink.flush.seconds`.
///
/// Keys that contain `.`, brackets, quotes or whitespace print quoted in brackets
/// (`tables.snapshots.overrides["public.orders"]`), so a table or pipeline name never reads as
/// several segments.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct YamlPath(Vec<Segment>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Segment {
    Key(String),
    Index(usize),
}

impl YamlPath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn key(&self, key: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.0.push(Segment::Key(key.into()));
        next
    }

    pub fn index(&self, index: usize) -> Self {
        let mut next = self.clone();
        next.0.push(Segment::Index(index));
        next
    }
}

fn needs_quoting(key: &str) -> bool {
    key.is_empty()
        || key
            .chars()
            .any(|c| matches!(c, '.' | '[' | ']' | '"') || c.is_whitespace())
}

impl fmt::Display for YamlPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::Key(k) if needs_quoting(k) => write!(f, "[{k:?}]")?,
                Segment::Key(k) if i == 0 => f.write_str(k)?,
                Segment::Key(k) => write!(f, ".{k}")?,
                Segment::Index(n) => write!(f, "[{n}]")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prints_dotted_keys_and_quotes_names_with_dots() {
        let p = YamlPath::root()
            .key("pipelines")
            .key("orders-pg")
            .key("tables")
            .key("snapshots")
            .key("overrides")
            .key("public.orders");
        assert_eq!(
            p.to_string(),
            r#"pipelines.orders-pg.tables.snapshots.overrides["public.orders"]"#
        );
        let q = YamlPath::root()
            .key("globals")
            .key("cluster")
            .key("memberlist")
            .key("join_members")
            .index(2);
        assert_eq!(q.to_string(), "globals.cluster.memberlist.join_members[2]");
        assert_eq!(YamlPath::root().to_string(), "");
    }
}
