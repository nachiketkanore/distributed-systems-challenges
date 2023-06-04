use crate::Body::{AddOk, InitOk, ReadOk};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{BufRead, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use anyhow::Error;

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
        node_id: String,       // this node's id
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
        value: u64,
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
        Body::Init {node_id, node_ids, msg_id} => {
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
        },
        _ => {
            panic!("expected init message at first");
        }
    }
    // process init message

    let value_main = Arc::new(Mutex::new(0u64));
    let value_secondary = Arc::clone(&value_main);

    let handler = std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            let value = value_secondary.lock().unwrap();
            for cluster_node in &cluster_nodes {
                if *cluster_node != this_node {
                    let msg: Message = Message {
                        src: this_node.clone(),
                        dest: (*cluster_node).clone(),
                        body: Body::InternalMessage { value: *value },
                    };
                    let serialized_output = serde_json::to_string(&msg)?;
                    println!("{}", serialized_output);
                    // print_and_flush(msg)?;
                }
            }
            drop(value);
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    });

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
            }
            InitOk { .. } | ReadOk { .. } | AddOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
            Body::InternalMessage { value } => {
                let mut my_value = value_main.lock().unwrap();
                if *my_value < value {
                    *my_value = value;
                }
            }
        }
    }
    let _ = handler.join().expect("Failed to join thread");
    Ok(())
}
