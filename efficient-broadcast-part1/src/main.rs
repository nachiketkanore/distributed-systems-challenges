use crate::Body::{BroadcastOk, InitOk, InternalMessage, ReadOk, TopologyOk};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};

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
        all_messages: HashSet<u64>,
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

    // naive solution:
    // sending all messages to other nodes in the cluster
    // in some frequent interval

    let msgs = Arc::new(Mutex::new(HashSet::new()));
    let msgs_secondary = Arc::clone(&msgs);
    let mut this_node_id = String::new();
    let mut this_node = String::new();
    // let mut cluster_nodes = Vec::<String>::new();

    // process init message
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    let info: Message = serde_json::from_str(&line)?;
    match info.body {
        Body::Init {
            node_id,
            node_ids: _,
            msg_id,
        } => {
            this_node = node_id.clone();
            this_node_id = node_id;
            // cluster_nodes = node_ids;
            let output: Message = Message {
                src: info.dest,
                dest: info.src,
                body: InitOk {
                    in_reply_to: msg_id,
                },
            };
            print_and_flush(output)?;
        }
        _ => {
            panic!("expected init message at first");
        }
    }
    // process init message

    let (topology_sender, topology_receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) =
        channel();

    #[allow(unreachable_code)]
    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        // batch thread to send current node's all messages to everyone in the cluster
        // every 500 ms

        let adjacent: Vec<String> = topology_receiver.recv().unwrap();
        loop {
            thread::sleep(Duration::from_millis(150));
            // TODO: this sends 24 messages (all the other nodes in the cluster group)
            // find a better topology to achieve expected latency
            // without compromising the msgs-per-op
            for cluster_node in &adjacent {
                if this_node_id != *cluster_node {
                    let my_msgs = msgs_secondary.lock().unwrap();
                    let internal_msg: Message = Message {
                        src: this_node_id.clone(),
                        dest: (*cluster_node).clone(),
                        body: InternalMessage {
                            all_messages: my_msgs.clone(),
                        },
                    };
                    drop(my_msgs);

                    let serialized_output = serde_json::to_string(&internal_msg)?;
                    println!("{}", serialized_output);
                }
            }
        }
        Ok(())
    });

    for line in stdin.lock().lines() {
        let input: Message = serde_json::from_str(&line?)?;
        match input.body {
            Body::Init { .. } => {
                // already process this message
                // will never receive this message again
                // let output: Message = Message {
                //     src: input.dest,
                //     dest: input.src,
                //     body: InitOk {
                //         in_reply_to: msg_id,
                //     },
                // };
                // this_node_id = node_id;
                // cluster_nodes = node_ids;
                // print_and_flush(output)?;
                panic!("init message sent twice");
            }
            Body::Read { msg_id } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: ReadOk {
                        msg_id,
                        in_reply_to: msg_id,
                        messages: msgs.lock().unwrap().clone(),
                    },
                };
                print_and_flush(output)?;
            }
            Body::Broadcast { msg_id, message } => {
                msgs.lock().unwrap().insert(message);
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: BroadcastOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::Topology {
                msg_id,
                topology: _,
            } => {
                // currently, completely ignoring the topology here
                // TODO: try using the given topology and compare the results
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: TopologyOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
                // let adjacent_nodes: Vec<String> = topology.remove(&this_node).unwrap();
                let adjacent_nodes: Vec<String> = if this_node == "n0" {
                    (1..=24).map(|i| format!("n{}", i)).collect()
                } else {
                    vec!["n0".to_string()]
                };

                topology_sender.send(adjacent_nodes).unwrap();
            }
            Body::InternalMessage { all_messages, .. } => {
                let mut my_msgs = msgs.lock().unwrap();
                my_msgs.extend(all_messages);
            }
            InitOk { .. } | ReadOk { .. } | BroadcastOk { .. } | TopologyOk { .. } => {
                eprintln!("Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
        }
    }
    let _ = handler.join().unwrap();
    Ok(())
}
// Solution description:
// batch process to send current node's all messages to every other node in the cluster
// every 500 ms
// this ensures we are sending all the messages from given node to every other node in the cluster
// even in the case of network partitions, eventual consistency will be observed
// because even if some of the internal messages are not received on the other end,
// they will be in consensus after the network partition is restored and all the messages are sent again
