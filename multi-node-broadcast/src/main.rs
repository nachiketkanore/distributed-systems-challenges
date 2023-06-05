use crate::Body::{BroadcastOk, InitOk, InternalMessage, ReadOk, TopologyOk};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{BufRead, Write};

// generic type for json received for all problems
// use this as a template
#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Body {
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: u64,
    },
    Broadcast {
        message: u64,
        msg_id: u64,
    },
    BroadcastOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    Read {
        msg_id: u64,
    },
    ReadOk {
        msg_id: u64,
        in_reply_to: u64,
        messages: HashSet<u64>,
    },
    Topology {
        // topology: InnerTopology,
        topology: HashMap<String, Vec<String>>,
        msg_id: u64,
    },
    TopologyOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    Error {
        in_reply_to: u64,
        code: u64,
        text: String,
    },
    InternalMessage {
        msg_id: u64,
        new_message: u64,
    },
}

fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    let mut print_and_flush = |output: Message| -> anyhow::Result<()> {
        let serialized_output = serde_json::to_string(&output)?;
        writeln!(stdout, "{}", serialized_output)?;
        stdout.flush()?;
        Ok(())
    };

    let mut msgs = HashSet::new();
    let mut topology_graph: HashMap<String, Vec<String>> = HashMap::new();
    let mut this_node_id = String::new();
    let mut cluster_nodes = Vec::<String>::new();

    for line in stdin.lock().lines() {
        let input: Message = serde_json::from_str(&line?)?;
        match input.body {
            // this message is important
            // has info { node_id of current node, node_ids of all nodes in the current cluster }
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: InitOk {
                        in_reply_to: msg_id,
                    },
                };
                this_node_id = node_id;
                cluster_nodes = node_ids;
                print_and_flush(output)?;
            }
            Body::Read { msg_id } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: ReadOk {
                        msg_id,
                        in_reply_to: msg_id,
                        messages: msgs.clone(),
                    },
                };
                print_and_flush(output)?;
            }
            Body::Broadcast { msg_id, message } => {
                msgs.insert(message);
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: BroadcastOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
                // brute force - try 1
                // send internal message to all other nodes in the cluster
                // to add a new message in their state

                for cluster_node in &cluster_nodes {
                    if *cluster_node != this_node_id {
                        let internalMessage: Message = Message {
                            src: this_node_id.clone(),
                            dest: (*cluster_node).clone(),
                            body: InternalMessage {
                                msg_id,
                                new_message: message,
                            },
                        };
                        let serialized_output = serde_json::to_string(&internalMessage)?;
                        println!("{}", serialized_output);
                        // print_and_flush(internalMessage)?;
                    }
                }
            }
            Body::Topology { msg_id, topology } => {
                topology_graph = topology;
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: TopologyOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::InternalMessage { new_message, .. } => {
                msgs.insert(new_message);
            }
            InitOk { .. } | ReadOk { .. } | BroadcastOk { .. } | TopologyOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
        }
    }
    Ok(())
}
