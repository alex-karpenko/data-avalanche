//! Case plumbing: base configs as JSON values, edits by JSON pointer, and the case type.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::support::Files;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The rule reports an error.
    Error,
    /// The rule reports a warning.
    Warning,
    /// The rule reports nothing.
    Silent,
}

pub struct Fixture {
    pub doc: Value,
    pub env: BTreeMap<String, String>,
}

pub struct Case {
    pub rule: &'static str,
    pub expect: Expect,
    pub name: &'static str,
    pub build: Box<dyn Fn(&Files) -> Fixture>,
}

/// A case whose config needs no files and only the base environment.
pub fn case(
    rule: &'static str,
    expect: Expect,
    name: &'static str,
    doc: impl Fn() -> Value + 'static,
) -> Case {
    Case {
        rule,
        expect,
        name,
        build: Box::new(move |_| Fixture {
            doc: doc(),
            env: base_env(),
        }),
    }
}

/// A case that writes files or sets variables.
pub fn case_with(
    rule: &'static str,
    expect: Expect,
    name: &'static str,
    build: impl Fn(&Files) -> Fixture + 'static,
) -> Case {
    Case {
        rule,
        expect,
        name,
        build: Box::new(build),
    }
}

pub fn base_env() -> BTreeMap<String, String> {
    crate::support::env(&[("PW", "pw")])
}

/// PostgreSQL → Delta, pipeline `p` (id `p`).
pub fn pg_delta() -> Value {
    json!({
        "config_version": 1,
        "globals": { "state": { "bucket": "app-state" } },
        "pipelines": {
            "p": {
                "id": "p",
                "source": {
                    "host": "db.example.com", "db": "shop", "user": "cdc", "password": "${PW}",
                    "postgres": { "publication": "p_pub", "slot": "p_slot" }
                },
                "tables": { "include": ["public.orders"] },
                "sink": { "type": "delta", "bucket": "lake", "delta": {} }
            }
        }
    })
}

/// MySQL → Delta, pipeline `p`.
pub fn mysql_delta() -> Value {
    pg_delta()
        .set("/pipelines/p/source/type", json!("mysql"))
        .del("/pipelines/p/source/postgres")
        .set("/pipelines/p/source/mysql", json!({}))
        .set("/pipelines/p/tables/include", json!(["orders"]))
}

/// PostgreSQL → Iceberg with a REST catalog (no auth), pipeline `p`.
pub fn pg_iceberg() -> Value {
    pg_delta().set(
        "/pipelines/p/sink",
        json!({
            "type": "iceberg", "bucket": "lake",
            "iceberg": { "catalog": { "rest": { "url": "https://catalog.example.com" } } }
        }),
    )
}

pub fn bases() -> Vec<(&'static str, Value)> {
    vec![
        ("pg_delta", pg_delta()),
        ("mysql_delta", mysql_delta()),
        ("pg_iceberg", pg_iceberg()),
    ]
}

/// Edits by JSON pointer (`/pipelines/p/sink/type`). `set` creates missing objects on the way.
pub trait Edit: Sized {
    fn set(self, pointer: &str, value: Value) -> Self;
    fn del(self, pointer: &str) -> Self;
}

impl Edit for Value {
    fn set(mut self, pointer: &str, value: Value) -> Self {
        let mut node = &mut self;
        let keys: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
        let (last, parents) = keys.split_last().expect("non-empty pointer");
        for key in parents {
            let map = node.as_object_mut().expect("object on the path");
            node = map.entry(key.to_string()).or_insert_with(|| json!({}));
        }
        node.as_object_mut()
            .expect("object at the parent")
            .insert(last.to_string(), value);
        self
    }

    fn del(mut self, pointer: &str) -> Self {
        let (parent, key) = pointer.rsplit_once('/').expect("pointer with a parent");
        if let Some(Value::Object(map)) = self.pointer_mut(parent) {
            map.remove(key);
        }
        self
    }
}
