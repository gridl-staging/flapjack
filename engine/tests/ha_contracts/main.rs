// HA Contract Tests — executable contract target for Decision 0003.
//
// Each module maps 1:1 to a contract in:
//   engine/docs2/3_IMPLEMENTATION/decisions/active/0003_search_ha_ownership_freshness.md
//
// C1/C2/C3/C4/C5 are enforced regression locks at HEAD.

#[path = "../common/mod.rs"]
mod common;

mod c1_ownership;
mod c2_per_tenant_sequence;
mod c3_replica_freshness;
mod c4_restart_recovery;
mod c5_split_brain_precedence;
