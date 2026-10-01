//! The commit marker on a Delta table (Data-Flow §Delta Lake realization, D-IMPL-7).
//!
//! - **Fence:** every commit carries the app-transaction `txn { app_id:
//!   "data-avalanche/<pipeline_id>/owner", version: owner_epoch }`. Delta keeps the latest `txn`
//!   version per app in the table state (checkpoints included), so a foreign commit can't hide it.
//! - **Position:** the commit's `commitInfo` carries `data-avalanche.committed_position`,
//!   `.owner_epoch` and `.pipeline`. `commitInfo` is per commit, so the sink walks back from the
//!   latest version to the last commit of its own pipeline, past foreign ones (an external
//!   OPTIMIZE; gap DA-X-10).

use std::collections::HashMap;

use data_avalanche_core::SourcePosition;
use deltalake::logstore::LogStore;
use serde_json::Value;

pub const POSITION_KEY: &str = "data-avalanche.committed_position";
pub const EPOCH_KEY: &str = "data-avalanche.owner_epoch";
pub const PIPELINE_KEY: &str = "data-avalanche.pipeline";

/// How many commits the walk-back reads before giving up. Past it, the position is unknown and
/// the guard skips nothing; a replay is then an idempotent re-write.
pub const MAX_WALK_BACK: u64 = 1000;

/// The fence's app id.
pub fn app_id(pipeline_id: &str) -> String {
    format!("data-avalanche/{pipeline_id}/owner")
}

/// The `commitInfo` entries of one commit. `position` is `None` on a commit that doesn't finish
/// the change set (the truncate before its MERGE), so the walk-back never stops there.
pub fn commit_metadata(
    pipeline_id: &str,
    owner_epoch: u64,
    position: Option<&SourcePosition>,
) -> HashMap<String, Value> {
    let mut m = HashMap::from([
        (PIPELINE_KEY.to_string(), Value::from(pipeline_id)),
        (EPOCH_KEY.to_string(), Value::from(owner_epoch)),
    ]);
    if let Some(p) = position {
        m.insert(POSITION_KEY.to_string(), Value::from(p.to_tagged()));
    }
    m
}

/// The position `pipeline_id` recorded in one commit file (newline-delimited JSON actions), or
/// `None` when the commit isn't one of its positioned commits.
pub fn own_position(commit: &[u8], pipeline_id: &str) -> Result<Option<SourcePosition>, String> {
    for line in commit.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let action: Value =
            serde_json::from_slice(line).map_err(|e| format!("a commit line: {e}"))?;
        let Some(info) = action.get("commitInfo") else {
            continue;
        };
        if info.get(PIPELINE_KEY).and_then(Value::as_str) != Some(pipeline_id) {
            return Ok(None);
        }
        return match info.get(POSITION_KEY).and_then(Value::as_str) {
            Some(tagged) => SourcePosition::parse_tagged(tagged)
                .map(Some)
                .map_err(|e| format!("the committed position {tagged:?}: {e}")),
            None => Ok(None),
        };
    }
    Ok(None)
}

/// What the walk-back found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalkBack {
    Found {
        version: u64,
        position: SourcePosition,
    },
    /// Reached `stop` (the version of the own commit the caller already knows) without a newer one.
    Reached,
    /// No own commit in the log that is left (or within [`MAX_WALK_BACK`]).
    NotFound,
}

/// Reads commits from `latest` down and returns the newest one this pipeline positioned. It stops
/// at `stop` (the version of the own commit the caller already knows) and at a commit file that is
/// gone (expired-log cleanup).
pub async fn walk_back(
    log: &dyn LogStore,
    latest: u64,
    stop: Option<u64>,
    pipeline_id: &str,
) -> Result<WalkBack, String> {
    let mut version = latest;
    for _ in 0..MAX_WALK_BACK {
        if stop.is_some_and(|s| version <= s) {
            return Ok(WalkBack::Reached);
        }
        let Some(bytes) = log
            .read_commit_entry(version)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(WalkBack::NotFound);
        };
        if let Some(position) = own_position(&bytes, pipeline_id)? {
            return Ok(WalkBack::Found { version, position });
        }
        let Some(previous) = version.checked_sub(1) else {
            return Ok(WalkBack::NotFound);
        };
        version = previous;
    }
    Ok(WalkBack::NotFound)
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;

    use super::*;

    fn commit(info: Value) -> Vec<u8> {
        let mut out = br#"{"add":{"path":"x.parquet"}}"#.to_vec();
        out.push(b'\n');
        out.extend(serde_json::to_vec(&serde_json::json!({ "commitInfo": info })).unwrap());
        out.push(b'\n');
        out
    }

    #[test]
    fn the_app_id_names_the_pipeline() {
        assert_eq!(app_id("orders"), "data-avalanche/orders/owner");
    }

    #[test]
    fn own_commits_are_recognized_by_pipeline_and_position() {
        let pos = SourcePosition::Pg(Lsn(0x16B3748));
        let ours = commit(Value::Object(
            commit_metadata("orders", 3, Some(&pos))
                .into_iter()
                .collect(),
        ));
        assert_eq!(own_position(&ours, "orders").unwrap(), Some(pos.clone()));
        assert_eq!(own_position(&ours, "other").unwrap(), None);
        let unpositioned = commit(Value::Object(
            commit_metadata("orders", 3, None).into_iter().collect(),
        ));
        assert_eq!(own_position(&unpositioned, "orders").unwrap(), None);
        let foreign = commit(serde_json::json!({ "operation": "OPTIMIZE" }));
        assert_eq!(own_position(&foreign, "orders").unwrap(), None);
    }

    #[test]
    fn metadata_holds_the_tagged_position_and_the_epoch() {
        let m = commit_metadata("p", 7, Some(&SourcePosition::Pg(Lsn(16))));
        assert_eq!(m[POSITION_KEY], "pg:0/10");
        assert_eq!(m[EPOCH_KEY], 7);
        assert_eq!(m[PIPELINE_KEY], "p");
    }

    #[test]
    fn a_corrupt_marker_is_an_error_not_absent() {
        let bad = commit(serde_json::json!({ PIPELINE_KEY: "p", POSITION_KEY: "pg:nope" }));
        assert!(own_position(&bad, "p").is_err());
        assert!(own_position(b"{not json", "p").is_err());
    }
}
