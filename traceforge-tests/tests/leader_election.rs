//! TraceForge model of Raft leader election.
//!
//! Models 3 nodes communicating via message passing. Each node can:
//! - Start an election (become candidate, send RequestVote to others)
//! - Vote for a candidate (if it hasn't voted in this term)
//! - Become leader (if it receives a majority of votes)
//!
//! Key invariant: At most one leader per term (Election Safety).
//!
//! TraceForge explores all possible message orderings to verify the invariant.

use traceforge::thread;
use traceforge::{verify, Config, ConsType};

/// Messages in the Raft election protocol
#[derive(Clone, Debug, PartialEq)]
enum RaftMsg {
    /// RequestVote { candidate_id, term, reply_to }
    RequestVote {
        candidate_id: u32,
        term: u64,
        reply_to: traceforge::thread::ThreadId,
    },
    /// VoteResponse { voter_id, term, granted }
    VoteResponse {
        voter_id: u32,
        term: u64,
        granted: bool,
    },
}

/// Test: Two concurrent candidates competing for votes from a single voter.
/// Both node 1 and node 2 start elections in term 1.
/// Node 3 is a passive voter that can only vote for one of them.
///
/// Invariant: at most one leader per term.
///
/// With unordered (Bag) delivery, TraceForge explores all orderings of
/// RequestVote messages arriving at node 3, verifying that the "vote once
/// per term" rule prevents two leaders.
#[test]
fn two_candidates_election_safety() {
    let stats = verify(
        Config::builder()
            .with_cons_type(ConsType::Bag) // Unordered delivery — most adversarial
            .build(),
        || {
            // Node 3: passive voter
            let node3 = thread::spawn(move || -> bool {
                let mut voted_for: Option<u32> = None;

                // Receives exactly 2 RequestVote messages (one from each candidate)
                for _ in 0..2 {
                    let msg: RaftMsg = traceforge::recv_msg_block();
                    match msg {
                        RaftMsg::RequestVote {
                            candidate_id,
                            term,
                            reply_to,
                        } => {
                            if term >= 1
                                && (voted_for.is_none() || voted_for == Some(candidate_id))
                            {
                                // Grant vote
                                voted_for = Some(candidate_id);
                                traceforge::send_msg(
                                    reply_to,
                                    RaftMsg::VoteResponse {
                                        voter_id: 3,
                                        term,
                                        granted: true,
                                    },
                                );
                            } else {
                                // Deny vote (already voted for someone else)
                                traceforge::send_msg(
                                    reply_to,
                                    RaftMsg::VoteResponse {
                                        voter_id: 3,
                                        term,
                                        granted: false,
                                    },
                                );
                            }
                        }
                        _ => {}
                    }
                }
                false // node 3 never becomes leader
            });
            let node3_id = node3.thread().id();

            // Node 2: candidate
            let node2 = thread::spawn(move || -> bool {
                let my_term: u64 = 1;
                let my_id = thread::current_id();

                // Vote for self (1 vote) + request vote from node 3
                traceforge::send_msg(
                    node3_id,
                    RaftMsg::RequestVote {
                        candidate_id: 2,
                        term: my_term,
                        reply_to: my_id,
                    },
                );

                // Wait for response from node 3
                let msg: RaftMsg = traceforge::recv_msg_block();
                match msg {
                    RaftMsg::VoteResponse {
                        granted: true,
                        term,
                        ..
                    } if term == my_term => {
                        // Got vote from node 3 + self-vote = 2 votes = majority
                        true
                    }
                    _ => false,
                }
            });

            // Node 1 (main thread): candidate
            let my_term: u64 = 1;
            let my_id = thread::current_id();

            // Vote for self (1 vote) + request vote from node 3
            traceforge::send_msg(
                node3_id,
                RaftMsg::RequestVote {
                    candidate_id: 1,
                    term: my_term,
                    reply_to: my_id,
                },
            );

            // Wait for response from node 3
            let msg: RaftMsg = traceforge::recv_msg_block();
            let node1_is_leader = match msg {
                RaftMsg::VoteResponse {
                    granted: true,
                    term,
                    ..
                } if term == my_term => {
                    // Got vote from node 3 + self-vote = 2 votes = majority
                    true
                }
                _ => false,
            };

            let node2_is_leader = node2.join().unwrap();
            let _node3_done = node3.join().unwrap();

            // INVARIANT: At most one leader per term
            let leader_count =
                [node1_is_leader, node2_is_leader].iter().filter(|&&x| x).count();

            assert!(
                leader_count <= 1,
                "ELECTION SAFETY VIOLATED! {} leaders elected in term 1",
                leader_count
            );
        },
    );
    println!(
        "two_candidates_election_safety: explored {} executions, {} blocked",
        stats.execs, stats.block
    );
}

/// Test: Single candidate election (baseline — should always succeed).
/// Node 1 starts an election, node 2 just votes.
/// Invariant: node 1 always becomes leader.
#[test]
fn single_candidate_always_wins() {
    let stats = verify(
        Config::builder()
            .with_cons_type(ConsType::FIFO)
            .build(),
        || {
            // Node 2: voter
            let node2 = thread::spawn(move || {
                let msg: RaftMsg = traceforge::recv_msg_block();
                match msg {
                    RaftMsg::RequestVote {
                        candidate_id,
                        term,
                        reply_to,
                    } => {
                        // Always grant vote (no competing candidates)
                        traceforge::send_msg(
                            reply_to,
                            RaftMsg::VoteResponse {
                                voter_id: 2,
                                term,
                                granted: true,
                            },
                        );
                    }
                    _ => panic!("Unexpected message"),
                }
            });
            let node2_id = node2.thread().id();

            // Node 1 (main thread): candidate
            let my_id = thread::current_id();
            traceforge::send_msg(
                node2_id,
                RaftMsg::RequestVote {
                    candidate_id: 1,
                    term: 1,
                    reply_to: my_id,
                },
            );

            let msg: RaftMsg = traceforge::recv_msg_block();
            let is_leader = matches!(
                msg,
                RaftMsg::VoteResponse {
                    granted: true,
                    term: 1,
                    ..
                }
            );

            let _ = node2.join();

            assert!(is_leader, "Node 1 should always win with no competition");
        },
    );
    println!(
        "single_candidate_always_wins: explored {} executions, {} blocked",
        stats.execs, stats.block
    );
}
