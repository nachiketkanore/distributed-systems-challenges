use crate::Body::{BroadcastOk, InitOk, InternalMessage, ReadOk, TopologyOk};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{BufRead, Write};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;

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

    let (msg_sender, msg_receiver): (Sender<Message>, Receiver<Message>) = channel();

    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            for msg in msg_receiver.iter().take(50) {
                let serialized_output = serde_json::to_string(&msg)?;
                println!("{}", serialized_output);
            }
            std::thread::sleep(Duration::from_millis(400));
        }
        Ok(())
    });

    let mut msgs = HashSet::new();
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

                // better solution
                // send internal message to all other nodes in the cluster
                // to add a new message in their state
                // frequently send messages to all the other nodes in the cluster
                // using mpsc::channels for inter-thread communication
                for cluster_node in &cluster_nodes {
                    if *cluster_node != this_node_id {
                        let internal_msg: Message = Message {
                            src: this_node_id.clone(),
                            dest: (*cluster_node).clone(),
                            body: InternalMessage {
                                new_message: message,
                            },
                        };
                        msg_sender.send(internal_msg)?;
                        // let serialized_output = serde_json::to_string(&internalMessage)?;
                        // println!("{}", serialized_output);
                        // print_and_flush(internalMessage)?;
                    }
                }
            }
            Body::Topology { msg_id, .. } => {
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
    let _ = handler.join().unwrap();
    Ok(())
}
