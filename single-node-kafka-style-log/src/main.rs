use crate::Body::InitOk;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::{collections::HashMap, io};

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
    Send {
        msg_id: u64,
        key: String,
        msg: u64,
    },
    SendOk {
        offset: usize, // usize for now
        in_reply_to: u64,
    },
    Poll {
        msg_id: u64,
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<u64>>>,
        in_reply_to: u64,
    },
    CommitOffsets {
        msg_id: u64,
        offsets: HashMap<String, u64>,
    },
    CommitOffsetsOk {
        in_reply_to: u64,
    },
    ListCommittedOffsets {
        msg_id: u64,
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
        in_reply_to: u64,
    },
    Error {
        in_reply_to: u64,
        code: u64,
        text: String,
    },
}

fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();

    let print_and_flush = |output: Message| -> anyhow::Result<()> {
        let serialized_output = serde_json::to_string(&output)?;
        eprintln!("--------------------------------------------------");
        eprintln!("{}", serialized_output);
        println!("{}", serialized_output);
        io::stdout().flush()?;
        Ok(())
    };

    // data for this node
    let mut data: HashMap<String, Vec<u64>> = HashMap::new();
    let mut commited_offsets: HashMap<String, u64> = HashMap::new();

    for line in stdin.lock().lines() {
        eprintln!("{:#?}", data);
        let input: Message = serde_json::from_str(&line?)?;
        eprintln!("{:#?}", input);
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
            Body::Send { msg_id, key, msg } => {
                let idx = if data.contains_key(&key) {
                    let x = data.entry(key).or_insert(vec![msg]);
                    x.push(msg);
                    x.len()
                } else {
                    data.insert(key, vec![msg]);
                    1
                };

                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body::SendOk {
                        offset: idx - 1,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::Poll { msg_id, offsets } => {
                let mut msgs: HashMap<std::string::String, Vec<Vec<u64>>> = HashMap::new();
                for (key, offset) in offsets {
                    if let Some(values) = data.get(&key) {
                        // let response: Vec<Vec<u64>> = values
                        //     .iter()
                        //     .skip((offset - 1).try_into().unwrap())
                        //     .enumerate()
                        //     .map(|(id, val)| vec![id as u64 + 1, *val])
                        //     .take(3)
                        //     .collect();
                        let response: Vec<Vec<u64>> = values
                            .iter()
                            .enumerate()
                            .filter(|(id, _)| *id >= offset)
                            .map(|(id, val)| vec![id as u64, (*val as u64).try_into().unwrap()])
                            .take(10)
                            .collect();
                        msgs.insert(key, response);
                    }
                }
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body::PollOk {
                        msgs,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::CommitOffsets { msg_id, offsets } => {
                for (key, offset) in offsets {
                    commited_offsets
                        .entry(key)
                        .and_modify(|curr| *curr = (*curr).max(offset))
                        .or_insert(offset);
                }
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body::CommitOffsetsOk {
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            Body::ListCommittedOffsets { msg_id, keys } => {
                let mut response = HashMap::new();
                for key in keys {
                    if let Some(&val) = commited_offsets.get(&key) {
                        response.insert(key, val);
                    }
                }
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body::ListCommittedOffsetsOk {
                        offsets: response,
                        in_reply_to: msg_id,
                    },
                };
                print_and_flush(output)?;
            }
            InitOk { .. }
            | Body::SendOk { .. }
            | Body::PollOk { .. }
            | Body::CommitOffsetsOk { .. }
            | Body::ListCommittedOffsetsOk { .. } => {
                eprintln!("Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
        }
    }
    Ok(())
}
