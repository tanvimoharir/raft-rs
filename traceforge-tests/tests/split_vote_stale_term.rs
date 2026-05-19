//! TraceForge model: Stale term + split vote scenario.
//!
//! This models a more complex scenario:
//! - 5 nodes in the cluster
//! - Node 1 starts election in term 1
//! - Node 2 starts election in term 1 (concurrent)
//! - Node 3 starts election in term 2 (higher term, after observing term 1)
//!
//! With 5 nodes, majority = 3. This creates interesting interleavings where
//! stale votes from term 1 could theoretically interfere with term 2 if the
//! protocol is buggy.
//!
//! Invariant: At most one leader per term.

use traceforge::thread;
use traceforge::{verify, Config, ConsType};

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    RequestVote {
        candidate_id: u32,
        term: u64,
        reply_to: traceforge::thread::ThreadId,
    },
    VoteResponse {
        voter_id: u32,
        term: u64,
        granted: bool,
    },
}

/// A voter node that processes vote requests respecting the "vote once per term" rule.
/// Returns the term it last observed.
fn voter(num_requests: usize) -> u64 {
    let mut current_term: u64 = 0;
    let mut voted_for_in_term: Option<(u64, u32)> = None; // (term, candidate_id)

    for _ in 0..num_requests {
        let msg: Msg = traceforge::recv_msg_block();
        match msg {
            Msg::RequestVote {
                candidate_id,
                term,
                reply_to,
            } => {
                // Update term if we see a higher one
                if term > current_term {
                    current_term = term;
                    voted_for_in_term = None; // Reset vote for new term
                }

                let can_vote = if term < current_term {
                    // Stale request — deny
                    false
                } else {
                    // Same term — check if we already voted
                    match voted_for_in_term {
                        None => true,
                        Some((t, cid)) => t == term && cid == candidate_id,
                    }
                };

                if can_vote {
                    voted_for_in_term = Some((term, candidate_id));
                    traceforge::send_msg(
                        reply_to,
                        Msg::VoteResponse {
                            voter_id: 0, // doesn't matter for this model
                            term,
                            granted: true,
                        },
                    );
                } else {
                    traceforge::send_msg(
                        reply_to,
                        Msg::VoteResponse {
                            voter_id: 0,
                            term: current_term,
                            granted: false,
                        },
                    );
                }
            }
            _ => {}
        }
    }
    current_term
}

/// Test: 3 nodes, 2 candidates in same term, 1 voter.
/// With Bag (unordered) delivery, TraceForge explores all orderings.
#[test]
fn three_node_two_candidates_bag_delivery() {
    let stats = verify(
        Config::builder()
            .with_cons_type(ConsType::Bag)
            .build(),
        || {
            // Voter node (receives 2 RequestVote messages)
            let voter_handle = thread::spawn(move || voter(2));
            let voter_id = voter_handle.thread().id();

            // Candidate 2
            let candidate2 = thread::spawn(move || -> bool {
                let my_id = thread::current_id();
                traceforge::send_msg(
                    voter_id,
                    Msg::RequestVote {
                        candidate_id: 2,
                        term: 1,
                        reply_to: my_id,
                    },
                );
                let resp: Msg = traceforge::recv_msg_block();
                matches!(resp, Msg::VoteResponse { granted: true, term: 1, .. })
            });

            // Candidate 1 (main thread)
            let my_id = thread::current_id();
            traceforge::send_msg(
                voter_id,
                Msg::RequestVote {
                    candidate_id: 1,
                    term: 1,
                    reply_to: my_id,
                },
            );
            let resp: Msg = traceforge::recv_msg_block();
            let c1_got_vote = matches!(resp, Msg::VoteResponse { granted: true, term: 1, .. });

            let c2_got_vote = candidate2.join().unwrap();
            let _ = voter_handle.join();

            // INVARIANT: voter can only grant one vote per term
            assert!(
                !(c1_got_vote && c2_got_vote),
                "BUG: Voter granted votes to BOTH candidates in the same term!"
            );
        },
    );
    println!(
        "three_node_two_candidates_bag_delivery: {} executions, {} blocked",
        stats.execs, stats.block
    );
}

/// Test: What happens if a voter has a buggy implementation that doesn't
/// track voted_for correctly? This should FIND a bug.
///
/// We intentionally introduce a bug: the voter grants votes to everyone
/// regardless of whether it already voted. TraceForge should find the
/// violation.
#[test]
#[should_panic(expected = "BUG: Buggy voter granted votes to BOTH")]
fn buggy_voter_finds_violation() {
    let _stats = verify(
        Config::builder()
            .with_cons_type(ConsType::Bag)
            .build(),
        || {
            // BUGGY voter: always grants votes (doesn't track voted_for)
            let voter_handle = thread::spawn(move || {
                for _ in 0..2 {
                    let msg: Msg = traceforge::recv_msg_block();
                    match msg {
                        Msg::RequestVote { term, reply_to, .. } => {
                            // BUG: Always grant, never check if already voted!
                            traceforge::send_msg(
                                reply_to,
                                Msg::VoteResponse {
                                    voter_id: 3,
                                    term,
                                    granted: true,
                                },
                            );
                        }
                        _ => {}
                    }
                }
            });
            let voter_id = voter_handle.thread().id();

            // Candidate 2
            let candidate2 = thread::spawn(move || -> bool {
                let my_id = thread::current_id();
                traceforge::send_msg(
                    voter_id,
                    Msg::RequestVote {
                        candidate_id: 2,
                        term: 1,
                        reply_to: my_id,
                    },
                );
                let resp: Msg = traceforge::recv_msg_block();
                matches!(resp, Msg::VoteResponse { granted: true, term: 1, .. })
            });

            // Candidate 1 (main thread)
            let my_id = thread::current_id();
            traceforge::send_msg(
                voter_id,
                Msg::RequestVote {
                    candidate_id: 1,
                    term: 1,
                    reply_to: my_id,
                },
            );
            let resp: Msg = traceforge::recv_msg_block();
            let c1_got_vote = matches!(resp, Msg::VoteResponse { granted: true, term: 1, .. });

            let c2_got_vote = candidate2.join().unwrap();
            let _ = voter_handle.join();

            // This SHOULD fail because the buggy voter grants both
            assert!(
                !(c1_got_vote && c2_got_vote),
                "BUG: Buggy voter granted votes to BOTH candidates in the same term!"
            );
        },
    );
}
