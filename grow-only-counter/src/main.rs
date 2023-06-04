use std::collections::HashSet;
use crate::Body::{AddOk, InitOk, InternalMessage, InternalMessageOk, ReadOk};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{BufRead, Read, Write};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;
use anyhow::Error;

#[derive(Serialize, Deserialize, Debug, Eq, Hash, PartialEq)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug, Eq, Hash, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Body {
    Init {
        msg_id: u64,
        node_id: String,
        // this node's id
        node_ids: Vec<String>, // all nodes in the current cluster
    },
    InitOk {
        in_reply_to: u64,
    },
    Read {
        msg_id: u64,
    },
    ReadOk {
        msg_id: u64,
        in_reply_to: u64,
        value: u64,
    },
    Add {
        msg_id: u64,
        delta: u64,
    },
    AddOk {
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
        add_delta: u64,
    },
    InternalMessageOk {
        msg_id: u64
    },
}

fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    let mut print_and_flush = move |output: Message| -> anyhow::Result<()> {
        let serialized_output = serde_json::to_string(&output)?;
        writeln!(stdout, "{}", serialized_output)?;
        stdout.flush()?;
        Ok(())
    };

    let mut this_node = String::new();
    let mut cluster_nodes = Vec::<String>::new();

    // process init message
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    let info: Message = serde_json::from_str(&line)?;
    match info.body {
        Body::Init { node_id, node_ids, msg_id } => {
            this_node = node_id;
            cluster_nodes = node_ids;
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

    let value_main = Arc::new(Mutex::new(0u64));
    let acknowledged_messages_main = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let acknowledged_messages_secondary = Arc::clone(&acknowledged_messages_main);

    let (msg_sender, msg_receiver): (Sender<Message>, Receiver<Message>) = channel();

    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        let mut all_messages = HashSet::<Message>::new();
        loop {
            for msg in &all_messages {
                match msg.body {
                    Body::InternalMessage { msg_id, add_delta } => {
                        let checker = acknowledged_messages_secondary.lock().unwrap();
                        if !checker.contains(&msg_id) {
                            let serialized_output = serde_json::to_string(&msg)?;
                            println!("{}", serialized_output);
                        }
                    }
                    _ => panic!("invalid internal message sent")
                }
            }
            for msg in msg_receiver.iter().take(100) {
                match msg.body {
                    Body::InternalMessage { msg_id, add_delta } => {
                        let checker = acknowledged_messages_secondary.lock().unwrap();
                        if !checker.contains(&msg_id) {
                            let serialized_output = serde_json::to_string(&msg)?;
                            println!("{}", serialized_output);
                            // it might happen that this message is not propagated to other node
                            // in case of network partition
                            // let's store all messages that we are sending
                            all_messages.insert(msg);
                        } else {
                            // remove from all_messages
                            all_messages.remove(&msg);
                        }
                    }
                    _ => panic!("invalid internal message sent")
                }
                std::thread::sleep(Duration::from_millis(30));
            }

        }
        Ok(())
    });

    let mut my_msg_id = 0;

    for line in stdin.lock().lines() {
        let input: Message = serde_json::from_str(&line?)?;
        match input.body {
            Body::Init { .. } => { panic!("already done"); }
            Body::Read { msg_id } => {
                let value = value_main.lock().unwrap();
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: ReadOk {
                        msg_id,
                        in_reply_to: msg_id,
                        value: *value,
                    },
                };
                print_and_flush(output)?;
            }
            Body::Add { msg_id, delta, .. } => {
                let mut value = value_main.lock().unwrap();
                *value += delta;
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: AddOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;

                // send messages to all other nodes to add delta in their counter
                // using messaging queue for this
                for cluster_node in &cluster_nodes {
                    if *cluster_node != this_node {
                        let msg: Message = Message {
                            src: this_node.clone(),
                            dest: (*cluster_node).clone(),
                            body: InternalMessage {
                                msg_id: my_msg_id,
                                add_delta: delta,
                            },
                        };
                        msg_sender.send(msg)?;
                        my_msg_id += 1;
                    }
                }
            }
            InitOk { .. } | ReadOk { .. } | AddOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
            Body::InternalMessage { msg_id, add_delta } => {
                let mut my_value = value_main.lock().unwrap();
                *my_value += add_delta;
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: InternalMessageOk {
                        msg_id
                    },
                };
                print_and_flush(output)?;
            }
            Body::InternalMessageOk { msg_id } => {
                let mut checker = acknowledged_messages_main.lock().unwrap();
                checker.insert(msg_id);
                // ack_sender.send(msg_id);
                // acknowledged_messages.insert(msg_id);
            }
        }
    }
    let _ = handler.join().expect("Failed to join thread");
    Ok(())
}
