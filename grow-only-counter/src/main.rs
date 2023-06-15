use crate::Body::{AddOk, InitOk, InternalMessage, ReadOk};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};

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
        latest_value: u64,
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

    let this_node;
    let cluster_nodes;
    let this_node_clone;
    let cluster_nodes_clone;

    // process init message - start
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    let info: Message = serde_json::from_str(&line)?;
    match info.body {
        Body::Init {
            node_id,
            node_ids,
            msg_id,
        } => {
            this_node = node_id;
            this_node_clone = this_node.clone();
            cluster_nodes = node_ids;
            cluster_nodes_clone = cluster_nodes.clone();
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
    // process init message - end

    let my_value = Arc::new(Mutex::new(0u64));
    let my_value_secondary = Arc::clone(&my_value);
    let mut latest_values_for = HashMap::<String, u64>::new();

    #[allow(unreachable_code)]
    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        let mut my_msg_id = 0;
        loop {
            thread::sleep(Duration::from_millis(20));
            for cluster_node in &cluster_nodes_clone {
                if *cluster_node != this_node_clone {
                    let msg: Message = Message {
                        src: this_node_clone.clone(),
                        dest: cluster_node.clone(),
                        body: InternalMessage {
                            msg_id: my_msg_id,
                            latest_value: *my_value_secondary.lock().unwrap(),
                        },
                    };
                    my_msg_id += 1;
                    let serialized_msg = serde_json::to_string(&msg)?;
                    println!("{}", serialized_msg);
                }
            }
        }
        Ok(())
    });

    for line in stdin.lock().lines() {
        let input: Message = serde_json::from_str(&line?)?;
        match input.body {
            Body::Init { .. } => {
                panic!("already done");
            }
            Body::Read { msg_id } => {
                let mut sum_of_all = *my_value.lock().unwrap();
                for cluster_node in &cluster_nodes {
                    if *cluster_node != this_node {
                        if let Some(value) = latest_values_for.get(cluster_node) {
                            sum_of_all += value;
                        }
                    }
                }

                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: ReadOk {
                        msg_id,
                        in_reply_to: msg_id,
                        value: sum_of_all,
                    },
                };
                print_and_flush(output)?;
            }
            Body::Add { msg_id, delta, .. } => {
                *my_value.lock().unwrap() += delta;
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: AddOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            InitOk { .. } | ReadOk { .. } | AddOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
            Body::InternalMessage {
                msg_id: _,
                latest_value,
            } => {
                *latest_values_for.entry(input.src).or_insert(0) = latest_value;
            }
        }
    }
    let _ = handler.join().expect("Failed to join thread");
    Ok(())
}
