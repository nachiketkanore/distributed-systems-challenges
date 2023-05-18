use crate::Body::{BroadcastOk, InitOk, ReadOk, TopologyOk};
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize)]
struct InnerTopology {
    #[serde(rename = "n1")]
    n1: Vec<String>,
    #[serde(rename = "n2")]
    n2: Vec<String>,
    #[serde(rename = "n3")]
    n3: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Body {
    // this type = "init" is received at the start
    #[serde(rename = "init")]
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    // response for the type = "init"
    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: u64 },

    // type = "broadcast" for this problem
    #[serde(rename = "broadcast")]
    Broadcast { message: u64, msg_id: u64 },

    // response for the type = "broadcast"
    #[serde(rename = "broadcast_ok")]
    BroadcastOk { msg_id: u64, in_reply_to: u64 },

    // type = "read" for this problem
    #[serde(rename = "read")]
    Read { msg_id: u64 },

    // response for the type = "read"
    #[serde(rename = "read_ok")]
    ReadOk {
        msg_id: u64,
        in_reply_to: u64,
        messages: Vec<u64>,
    },

    // type = "topology" for this problem
    #[serde(rename = "topology")]
    Topology {
        // topology: InnerTopology,
        topology: std::collections::HashMap<String, Vec<String>>,
        msg_id: u64,
    },

    // response for the type = "read"
    #[serde(rename = "topology_ok")]
    TopologyOk { msg_id: u64, in_reply_to: u64 },

    // type = "error" received if something goes wrong
    #[serde(rename = "error")]
    Error {
        in_reply_to: u64,
        code: u64,
        text: String,
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

    let mut messages: Vec<u64> = Vec::new();

    for line in stdin.lock().lines() {
        let input: Message = serde_json::from_str(&line?)?;
        match input.body {
            Body::Init { msg_id, .. } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: InitOk {
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::Read { msg_id } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: ReadOk {
                        msg_id,
                        in_reply_to: msg_id,
                        messages: messages.clone(),
                    },
                };
                print_and_flush(output)?;
            }
            Body::Broadcast { msg_id, message } => {
                messages.push(message);
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
            InitOk { .. }
            | ReadOk { .. }
            | BroadcastOk { .. }
            | TopologyOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
        }
    }
    Ok(())
}
