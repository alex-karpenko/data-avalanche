//! One walker over the raw model (config §11 Phase 2 "empty is absent", §8.6 "`defaults` on its
//! own"). [`Walk`] is implemented for every raw type the same way [`crate::merge::Merge`] is:
//! `walk_fields!` generates it for a struct from a field list — `merge_struct!` invokes it with
//! the same list it uses for `Merge`, so the two can't drift there, and this module adds the
//! handful of raw structs `merge_struct!` never covers (the union types with a hand-written
//! `Merge`, and `RawPartitionField`, which is never merged on its own). Either way,
//! `walk_fields!` destructures the struct with no `..` (`let $t { $($f),+ } = self;`), so the
//! list is also tied to the compiler directly: a field the invocation omits fails to compile,
//! whether or not `merge_struct!` happens to cover that struct too.
//!
//! Two operations:
//! - [`Walk::drop_empty`]: an optional leaf whose resolved text is empty becomes absent — `None`
//!   for a plain optional, and for a tri-state field `Some(Some(""))` becomes `None` (absent, not
//!   `null`). A map entry whose value leaf is empty is dropped from the map. List elements are
//!   left alone: an empty string in a list is still whatever the list's own rule says about it
//!   (e.g. an empty glob is still an error), never "absent".
//! - [`Walk::check_values`]: coerces every leaf present, reporting `TYPE` / the leaf's own V-rule /
//!   `V-063` at that leaf's path (§8.6) — no presence check, no cross-field rule.

use std::collections::BTreeMap;

use crate::coerce::LeafType;
use crate::ctx::{At, Ctx};
use crate::leaf::{Leaf, Tri};
use crate::raw::*;

pub(crate) trait Walk {
    fn drop_empty(&mut self, ctx: &Ctx);
    fn check_values(&self, ctx: &mut Ctx, at: &At);
}

/// Maps a raw struct's field identifier to its YAML key: every field's Rust name matches, except
/// `kind`, which every discriminated struct (`RawSource`, `RawSink`, `RawCatalog`,
/// `RawDeltaCatalog`) renames to `type` (`#[serde(rename = "type")]`).
macro_rules! walk_key {
    (kind) => {
        "type"
    };
    ($f:ident) => {
        stringify!($f)
    };
}
pub(crate) use walk_key;

/// §11 Phase 2 exception: a `prefix` field that resolves to `""` keeps the empty prefix — the
/// bucket root (§10) — rather than being dropped as absent. `walk_fields!` routes every field's
/// `drop_empty` call through here so the one exception lives in one place, matched on the field's
/// own identifier: it fires for every raw struct's `prefix` field alike (`globals.state.prefix`,
/// `sink.prefix`, the pipeline `state.prefix`, `catalog.rest.prefix`) and nowhere else.
/// `check_values` is unaffected — it doesn't route through this macro.
macro_rules! walk_drop_empty_field {
    (prefix, $val:expr, $ctx:expr) => {
        let _ = ($val, $ctx);
    };
    ($f:ident, $val:expr, $ctx:expr) => {
        $crate::walk::Walk::drop_empty($val, $ctx);
    };
}
pub(crate) use walk_drop_empty_field;

/// Generates `Walk` for a raw struct and for `Option<struct>`, from the same field list
/// `merge_struct!` uses for `Merge` (it calls this macro with its own list) or, for a struct
/// `merge_struct!` never covers, from a matching call added here.
macro_rules! walk_fields {
    ($t:ident { $($f:ident),+ $(,)? }) => {
        impl $crate::walk::Walk for $t {
            // Destructured with every field named and no `..`: adding a field to `$t` without
            // adding it here fails to compile, instead of silently never being walked.
            fn drop_empty(&mut self, ctx: &$crate::ctx::Ctx) {
                let $t { $($f),+ } = self;
                $( $crate::walk::walk_drop_empty_field!($f, $f, ctx); )+
            }
            fn check_values(&self, ctx: &mut $crate::ctx::Ctx, at: &$crate::ctx::At) {
                let $t { $($f),+ } = self;
                $( $crate::walk::Walk::check_values(
                    $f, ctx, &at.key($crate::walk::walk_key!($f)),
                ); )+
            }
        }
        impl $crate::walk::Walk for Option<$t> {
            fn drop_empty(&mut self, ctx: &$crate::ctx::Ctx) {
                if let Some(v) = self {
                    $crate::walk::Walk::drop_empty(v, ctx);
                }
            }
            fn check_values(&self, ctx: &mut $crate::ctx::Ctx, at: &$crate::ctx::At) {
                if let Some(v) = self {
                    $crate::walk::Walk::check_values(v, ctx, at);
                }
            }
        }
    };
}
pub(crate) use walk_fields;

// ---- leaf shapes (every field in `raw.rs` is one of these, or a struct `walk_fields!` covers) ----

/// A plain optional leaf: empty resolved text means absent.
impl<T: LeafType> Walk for Option<Leaf<T>> {
    fn drop_empty(&mut self, ctx: &Ctx) {
        if self
            .as_ref()
            .is_some_and(|l| ctx.peek_text(l).is_some_and(|t| t.is_empty()))
        {
            *self = None;
        }
    }

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        if let Some(l) = self {
            ctx.value(l, at);
        }
    }
}

/// A tri-state leaf (§9.4.1): an explicit value whose resolved text is empty is absent (use the
/// default), not `null` (disabled) — those are different outcomes, so this can't just fall through
/// to the plain-leaf case. Explicit `null` (`Some(None)`) is untouched: it isn't empty, it's
/// already "disabled".
impl<T: LeafType> Walk for Tri<T> {
    fn drop_empty(&mut self, ctx: &Ctx) {
        if let Some(Some(l)) = self
            && ctx.peek_text(l).is_some_and(|t| t.is_empty())
        {
            *self = None;
        }
    }

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        if let Some(Some(l)) = self {
            ctx.value(l, at);
        }
    }
}

/// A list of leaves (`join_members`, `bind_addr`): elements are left alone by `drop_empty`.
impl<T: LeafType> Walk for Option<Vec<Leaf<T>>> {
    fn drop_empty(&mut self, _ctx: &Ctx) {}

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        for (i, l) in self.iter().flatten().enumerate() {
            ctx.value(l, &at.index(i));
        }
    }
}

/// A tri-state list (`tables.include`/`exclude`, §9.4.1 applied to a list): absent or explicit
/// `null` stay as they are; elements of a present list are left alone by `drop_empty` — an empty
/// glob is still an error, never "absent" (§11 Phase 2).
impl<T: LeafType> Walk for Option<Option<Vec<Leaf<T>>>> {
    fn drop_empty(&mut self, _ctx: &Ctx) {}

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        if let Some(Some(items)) = self {
            for (i, l) in items.iter().enumerate() {
                ctx.value(l, &at.index(i));
            }
        }
    }
}

/// A map of scalar leaves (`snapshots.overrides`, `column_type_overrides`, `table_properties`): a
/// value leaf that resolves empty drops its whole entry.
impl<T: LeafType> Walk for Option<BTreeMap<String, Leaf<T>>> {
    fn drop_empty(&mut self, ctx: &Ctx) {
        if let Some(m) = self {
            m.retain(|_, l| !ctx.peek_text(l).is_some_and(|t| t.is_empty()));
        }
    }

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        for (k, l) in self.iter().flatten() {
            ctx.value(l, &at.key(k.as_str()));
        }
    }
}

/// A map of lists of leaves (`role_mapping`, `primary_key`): list elements are left alone, same as
/// a plain list.
impl<T: LeafType> Walk for Option<BTreeMap<String, Vec<Leaf<T>>>> {
    fn drop_empty(&mut self, _ctx: &Ctx) {}

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        for (k, items) in self.iter().flatten() {
            let at = at.key(k.as_str());
            for (i, l) in items.iter().enumerate() {
                ctx.value(l, &at.index(i));
            }
        }
    }
}

/// `tables.partitioning`: a map of lists of *structs*, not leaves — the only field of this shape,
/// so it gets its own (non-generic) impl rather than a `S: Walk` blanket that would overlap the
/// leaf-shaped impls above.
impl Walk for Option<BTreeMap<String, Vec<RawPartitionField>>> {
    fn drop_empty(&mut self, ctx: &Ctx) {
        if let Some(m) = self.as_mut() {
            for fields in m.values_mut() {
                for f in fields.iter_mut() {
                    f.drop_empty(ctx);
                }
            }
        }
    }

    fn check_values(&self, ctx: &mut Ctx, at: &At) {
        for (k, fields) in self.iter().flatten() {
            let at = at.key(k.as_str());
            for (i, f) in fields.iter().enumerate() {
                f.check_values(ctx, &at.index(i));
            }
        }
    }
}

// ---- raw structs `merge_struct!` doesn't cover ----

// `globals` (§6): never merged (`defaults` has no globals-shaped fields), so `merge_struct!` never
// runs for these.
walk_fields!(RawGlobals {
    cloud,
    state,
    api,
    log,
    cluster,
    tracing,
    azure
});
walk_fields!(RawGlobalState { bucket, prefix });
walk_fields!(RawApi {
    address,
    auth,
    user,
    password,
    credentials_file,
    jwt,
    oauth2,
    role_mapping,
    tls
});
walk_fields!(RawJwt {
    issuer,
    audience,
    jwks_url,
    roles_claim
});
walk_fields!(RawApiOauth2 {
    issuer,
    audience,
    jwks_url,
    discovery_url,
    roles_claim
});
walk_fields!(RawApiTls { enabled, cert, key });
walk_fields!(RawLog { level, format });
walk_fields!(RawCluster {
    tokens,
    lock_backend,
    lease_duration,
    lease_renewal_interval,
    lease_safety_margin,
    rebalance_stabilization,
    max_concurrent_handoffs,
    max_concurrent_takeovers,
    drain_timeout,
    discovery,
    discovery_dns_name,
    memberlist
});
walk_fields!(RawMemberlist {
    join_members,
    node_name,
    advertise_addr,
    advertise_port,
    bind_addr,
    bind_port,
    packet_dial_timeout,
    packet_write_timeout,
    compression_enabled,
    gossip_interval,
    gossip_nodes,
    probe_interval,
    probe_timeout,
    dead_node_reclaim_time,
    push_pull_interval,
    retransmit_factor,
    lifeguard_enabled,
    tls
});
walk_fields!(RawMemberlistTls {
    enabled,
    cert,
    key,
    ca
});
walk_fields!(RawTracing {
    otlp_endpoint,
    sample_ratio
});
walk_fields!(RawAzure { account_name });

// `pipelines`/`defaults` (§7, §9): the three union types have a hand-written `Merge` (config §8.4)
// and so never went through `merge_struct!`; `RawPartitionField` is never merged as a struct at
// all (`tables.partitioning` lists are replaced wholesale, §8.3). `Walk` doesn't care about union
// branches — it visits every leaf present, on both sides, the same as everywhere else.
walk_fields!(RawSource {
    kind,
    host,
    port,
    db,
    user,
    password,
    credentials_file,
    ssl_mode,
    ssl_cert,
    ssl_key,
    ssl_ca,
    timeouts,
    toast_strategy,
    postgres,
    mysql
});
walk_fields!(RawSink {
    kind,
    write_mode,
    bucket,
    prefix,
    flush,
    max_transaction_batch_rows,
    max_transaction_buffer_bytes,
    buffer_capacity_rows,
    metrics,
    iceberg,
    delta
});
walk_fields!(RawCatalog {
    kind,
    glue,
    hive,
    rest,
    retry
});
walk_fields!(RawPipelineState { bucket, prefix });
walk_fields!(RawPartitionField {
    column,
    transform,
    n,
    width
});

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::path::YamlPath;
    use crate::secret::FingerprintKey;

    fn at() -> At {
        At::config(YamlPath::root().key("x"))
    }

    #[test]
    fn drop_empty_turns_a_literal_empty_optional_leaf_into_absent() {
        let mut leaf: Option<Leaf<String>> = Some(Leaf::new(""));
        leaf.drop_empty(&Ctx::structural());
        assert_eq!(leaf, None);
    }

    #[test]
    fn drop_empty_leaves_an_unresolved_template_alone_in_the_structural_pass() {
        // Only a literal `""` is empty in the structural pass (§11 Phase 2); an unresolved
        // template isn't known to be empty yet.
        let mut leaf: Option<Leaf<String>> = Some(Leaf::new("${X}"));
        leaf.drop_empty(&Ctx::structural());
        assert!(leaf.is_some());
    }

    #[test]
    fn drop_empty_turns_a_tri_state_leaf_that_resolves_empty_into_fully_absent() {
        let env: BTreeMap<String, String> = BTreeMap::new();
        let key = FingerprintKey::from_bytes([1; 32]);
        let mut tri: Tri<u64> = Some(Some(Leaf::new("${S:-}")));
        tri.drop_empty(&Ctx::resolving(&env, &key));
        assert_eq!(
            tri, None,
            "empty means absent (apply the default), not null"
        );
    }

    #[test]
    fn drop_empty_leaves_an_explicit_null_tri_state_alone() {
        let mut tri: Tri<u64> = Some(None);
        tri.drop_empty(&Ctx::structural());
        assert_eq!(tri, Some(None));
    }

    #[test]
    fn drop_empty_leaves_list_elements_alone() {
        // An empty list element (e.g. a glob) is still whatever the list's own rule says about
        // it, never "absent".
        let mut list: Option<Vec<Leaf<String>>> = Some(vec![Leaf::new("")]);
        list.drop_empty(&Ctx::structural());
        assert_eq!(list, Some(vec![Leaf::new("")]));
    }

    #[test]
    fn drop_empty_removes_an_empty_map_entry() {
        let mut map: Option<BTreeMap<String, Leaf<String>>> =
            Some([("k".to_string(), Leaf::new(""))].into());
        map.drop_empty(&Ctx::structural());
        assert_eq!(map, Some(BTreeMap::new()));
    }

    #[test]
    fn drop_empty_keeps_a_literal_empty_prefix_field_but_still_drops_an_empty_bucket() {
        // §11 Phase 2 exception: `prefix` (here `RawPipelineState::prefix`) keeps `""` — the
        // bucket root (§10) — while every other field on the same struct follows the general
        // "empty is absent" rule.
        let mut state = RawPipelineState {
            bucket: Some(Leaf::new("")),
            prefix: Some(Leaf::new("")),
        };
        state.drop_empty(&Ctx::structural());
        assert_eq!(
            state.prefix,
            Some(Leaf::new("")),
            "a prefix field that resolves to \"\" stays present, not absent"
        );
        assert_eq!(
            state.bucket, None,
            "bucket has no such exception: empty still means absent"
        );
    }

    #[test]
    fn check_values_reports_type_at_the_leaf_path() {
        let mut ctx = Ctx::structural();
        let leaf: Option<Leaf<u64>> = Some(Leaf::new("abc"));
        leaf.check_values(&mut ctx, &at());
        assert_eq!(ctx.report().errors[0].rule, "TYPE");
        assert_eq!(ctx.report().errors[0].path, "x");
    }

    #[test]
    fn check_values_is_silent_on_an_unresolved_template() {
        let mut ctx = Ctx::structural();
        let leaf: Option<Leaf<u64>> = Some(Leaf::new("${X}"));
        leaf.check_values(&mut ctx, &at());
        assert!(ctx.report().is_ok());
    }

    #[test]
    fn check_values_recurses_a_struct_through_the_kind_to_type_key_mapping() {
        let mut ctx = Ctx::structural();
        let sink = Some(RawSink {
            kind: Some(Leaf::new("not-a-sink-format")),
            ..Default::default()
        });
        sink.check_values(&mut ctx, &at());
        let e = &ctx.report().errors[0];
        assert_eq!(e.path, "x.type", "the `kind` field's YAML key is `type`");
    }
}
