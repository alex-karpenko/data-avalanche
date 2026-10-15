//! MySQL partial JSON updates (`binlog_row_value_options = PARTIAL_JSON`, Q-M7a-3): an UPDATE by
//! `JSON_SET`, `JSON_REPLACE` or `JSON_REMOVE` logs the column's after image as a list of diffs
//! against its before image, which `binlog_row_image = FULL` carries whole. The source applies them
//! and emits the full new document. Measured on MySQL 8.4 (2026-10-14): paths are normalized
//! (`$`, `.key`, `."quoted key"` as a JSON string, `[n]`); an INSERT at an array index past the end
//! appends; auto-wrapping a scalar logs a REPLACE of the wrapped array; a statement that changes
//! nothing logs no diff. A whole-document set or `JSON_ARRAY_APPEND` logs the full value instead.

use mysql_async::binlog::jsondiff::{JsonDiff, JsonDiffOperation};
use serde_json::Value;

/// One step of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Leg {
    Key(String),
    Index(usize),
}

/// A normalized MySQL JSON path, `$` first.
fn parse(path: &str) -> Result<Vec<Leg>, String> {
    let bad = |why: &str| format!("JSON path {path:?}: {why}");
    let rest = path.strip_prefix('$').ok_or_else(|| bad("no leading $"))?;
    let mut legs = vec![];
    let mut s = rest;
    while !s.is_empty() {
        if let Some(r) = s.strip_prefix('.') {
            if r.starts_with('"') {
                // A JSON string: find its closing quote past escapes.
                let mut end = None;
                let mut escaped = false;
                for (i, c) in r.char_indices().skip(1) {
                    match (c, escaped) {
                        ('\\', false) => escaped = true,
                        ('"', false) => {
                            end = Some(i);
                            break;
                        }
                        _ => escaped = false,
                    }
                }
                let end = end.ok_or_else(|| bad("an unterminated quoted key"))?;
                let key: String =
                    serde_json::from_str(&r[..=end]).map_err(|_| bad("a quoted key"))?;
                legs.push(Leg::Key(key));
                s = &r[end + 1..];
            } else {
                let end = r.find(['.', '[']).unwrap_or(r.len());
                if end == 0 {
                    return Err(bad("an empty key"));
                }
                legs.push(Leg::Key(r[..end].to_owned()));
                s = &r[end..];
            }
        } else if let Some(r) = s.strip_prefix('[') {
            let end = r.find(']').ok_or_else(|| bad("an unterminated index"))?;
            let i = r[..end]
                .parse()
                .map_err(|_| bad("an index that isn't a number"))?;
            legs.push(Leg::Index(i));
            s = &r[end + 1..];
        } else {
            return Err(bad("a step that is neither .key nor [n]"));
        }
    }
    Ok(legs)
}

/// Applies one diff to `doc`.
pub(crate) fn apply(
    doc: &mut Value,
    op: JsonDiffOperation,
    path: &str,
    value: Option<Value>,
) -> Result<(), String> {
    let legs = parse(path)?;
    let missing = || format!("JSON path {path:?} isn't in the before image");
    let Some((last, parents)) = legs.split_last() else {
        // `$`: the whole document.
        return match (op, value) {
            (JsonDiffOperation::REMOVE, _) => Err("a REMOVE of the whole document".into()),
            (_, Some(v)) => {
                *doc = v;
                Ok(())
            }
            (_, None) => Err(format!("a {op:?} of {path:?} without a value")),
        };
    };
    let mut at = doc;
    for leg in parents {
        at = match (leg, at) {
            (Leg::Key(k), Value::Object(m)) => m.get_mut(k).ok_or_else(missing)?,
            (Leg::Index(i), Value::Array(a)) => a.get_mut(*i).ok_or_else(missing)?,
            _ => return Err(missing()),
        };
    }
    let need = |v: Option<Value>| v.ok_or_else(|| format!("a {op:?} of {path:?} without a value"));
    match (op, last, at) {
        (JsonDiffOperation::REMOVE, Leg::Key(k), Value::Object(m)) => {
            m.remove(k);
        }
        (JsonDiffOperation::REMOVE, Leg::Index(i), Value::Array(a)) if *i < a.len() => {
            a.remove(*i);
        }
        (_, Leg::Key(k), Value::Object(m)) if op != JsonDiffOperation::REMOVE => {
            m.insert(k.clone(), need(value)?);
        }
        (JsonDiffOperation::REPLACE, Leg::Index(i), Value::Array(a)) if *i < a.len() => {
            a[*i] = need(value)?;
        }
        (JsonDiffOperation::INSERT, Leg::Index(i), Value::Array(a)) => {
            let v = need(value)?;
            match *i < a.len() {
                true => a.insert(*i, v),
                false => a.push(v),
            }
        }
        _ => return Err(missing()),
    }
    Ok(())
}

/// Applies `diffs` in order to the before image `doc`.
pub(crate) fn apply_all(mut doc: Value, diffs: &[JsonDiff<'_>]) -> Result<Value, String> {
    for d in diffs {
        let value = d
            .value()
            .map(|v| Value::try_from(v.clone()))
            .transpose()
            .map_err(|e| format!("a diff value the decoder can't read: {e}"))?;
        apply(&mut doc, d.operation(), &d.path_str(), value)?;
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use JsonDiffOperation::*;
    use serde_json::json;

    fn start() -> Value {
        json!({"a": 1, "b": "long", "arr": [10, 20, 30], "o": {"p": {"q": 0}},
               "k e y": 1, "qu\"ote": 2, "ключ": 3, "s": 5})
    }

    /// Each measured statement's diffs give the server's document.
    #[test]
    fn measured_diffs_give_the_servers_document() {
        type Diff<'a> = (JsonDiffOperation, &'a str, Option<Value>);
        let cases: Vec<(Vec<Diff>, Value)> = vec![
            (vec![(REPLACE, "$.a", Some(json!(2)))], json!({"a": 2})),
            (
                vec![(INSERT, "$.new", Some(json!("x")))],
                json!({"new": "x"}),
            ),
            (vec![(REMOVE, "$.b", None)], json!({"b": null})),
            (
                vec![(REPLACE, "$.arr[1]", Some(json!(9)))],
                json!({"arr": [10, 9, 30]}),
            ),
            (
                vec![(INSERT, "$.arr[10]", Some(json!(7)))],
                json!({"arr": [10, 20, 30, 7]}),
            ),
            (vec![(REMOVE, "$.arr[0]", None)], json!({"arr": [20, 30]})),
            (
                vec![(REPLACE, r#"$."k e y""#, Some(json!(11)))],
                json!({"k e y": 11}),
            ),
            (
                vec![(REPLACE, r#"$."qu\"ote""#, Some(json!(12)))],
                json!({"qu\"ote": 12}),
            ),
            (
                vec![(REPLACE, r#"$."ключ""#, Some(json!(13)))],
                json!({"ключ": 13}),
            ),
            (
                vec![
                    (REPLACE, "$.o.p.q", Some(json!(1))),
                    (INSERT, "$.o.p.r", Some(json!(2))),
                ],
                json!({"o": {"p": {"q": 1, "r": 2}}}),
            ),
            (
                vec![(REPLACE, "$.s", Some(json!([5, 6])))],
                json!({"s": [5, 6]}),
            ),
            (
                vec![(REPLACE, "$.arr[0]", Some(json!({"x": 1})))],
                json!({"arr": [{"x": 1}, 20, 30]}),
            ),
            (vec![], json!({})),
        ];
        for (diffs, changes) in cases {
            let mut want = start();
            for (k, v) in changes.as_object().unwrap() {
                match v {
                    Value::Null => want.as_object_mut().unwrap().remove(k),
                    v => want.as_object_mut().unwrap().insert(k.clone(), v.clone()),
                };
            }
            let mut doc = start();
            for (op, path, value) in diffs.clone() {
                apply(&mut doc, op, path, value).unwrap();
            }
            assert_eq!(doc, want, "{diffs:?}");
        }
    }

    #[test]
    fn the_whole_document_and_array_inserts() {
        let mut doc = start();
        apply(&mut doc, REPLACE, "$", Some(json!({"whole": 1}))).unwrap();
        assert_eq!(doc, json!({"whole": 1}));
        let mut a = json!([1, 2]);
        apply(&mut a, INSERT, "$[0]", Some(json!(0))).unwrap();
        assert_eq!(a, json!([0, 1, 2]));
    }

    #[test]
    fn a_diff_the_before_image_cant_take_is_an_error() {
        for (op, path) in [
            (REPLACE, "$.nope.x"),
            (REPLACE, "$.arr[9]"),
            (REMOVE, "$"),
            (REPLACE, "$.a.b"),
            (REPLACE, "a"),
            (REPLACE, "$.\"open"),
            (REPLACE, "$[x]"),
            (REPLACE, "$.arr[*]"),
        ] {
            let mut doc = start();
            assert!(
                apply(&mut doc, op, path, Some(json!(1))).is_err(),
                "{op:?} {path}"
            );
        }
        assert!(
            apply(&mut start(), INSERT, "$.n", None).is_err(),
            "no value"
        );
    }

    #[test]
    fn paths_parse_their_steps() {
        assert_eq!(
            parse(r#"$.a[2]."b.c"[0].d"#).unwrap(),
            vec![
                Leg::Key("a".into()),
                Leg::Index(2),
                Leg::Key("b.c".into()),
                Leg::Index(0),
                Leg::Key("d".into())
            ]
        );
        assert_eq!(parse("$").unwrap(), vec![]);
    }
}
