//! TraceForge-based concurrency exploration tests for tikv/raft-rs.
//!
//! This crate models the Raft protocol's message-passing layer using TraceForge's
//! systematic exploration engine. Instead of the sequential `Network::send()` harness,
//! we model each Raft node as a TraceForge thread that sends and receives messages
//! concurrently, allowing TraceForge's DPOR algorithm to exhaustively explore all
//! relevant message orderings.
//!
//! Key invariants to verify:
//! - At most one leader per term (Election Safety)
//! - Leader's log contains all committed entries (Leader Completeness)
//! - If two logs contain an entry with the same index and term, they are identical (Log Matching)
//! - A committed entry is present on a majority of servers

pub use raft;
pub use raft_proto;
pub use traceforge;
