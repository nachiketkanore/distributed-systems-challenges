use crate::Body::{EchoOk, InitOk};
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
    // type = "echo" for this problem
    #[serde(rename = "echo")]
    Echo { msg_id: u64, echo: String },
    // response for the type = "echo"
    #[serde(rename = "echo_ok")]
    EchoOk {
        msg_id: u64,
        in_reply_to: u64,
        echo: String,
    },
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
            Body::Echo { msg_id, echo, .. } => {
                let output: Message = Message {
                    src: input.dest,
                    dest: input.src,
                    body: EchoOk {
                        msg_id,
                        in_reply_to: msg_id,
                        echo,
                    },
                };
                print_and_flush(output)?;
            }
            Body::InitOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::EchoOk { .. } => {
                eprintln!("{}", "Impossible input");
            }
            Body::Error { text, .. } => {
                eprintln!("{}", text);
            }
        }
        // let serialized_input = serde_json::to_string(&input)?;
        // writeln!(stdout, "{}", serialized_input)?;
        // stdout.flush()?;
    }
    Ok(())
}
