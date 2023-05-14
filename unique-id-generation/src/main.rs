use serde::{Deserialize, Serialize};

// generic type for json received for all problems
// use this as a template
#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Body {
    // this type = "init" is received at the start
    #[serde(rename = "init")]
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    // response for the type = "init"
    #[serde(rename = "init_ok")]
    InitOk {
        in_reply_to: u64,
    },
    // type = "echo" for this problem
    #[serde(rename = "echo")]
    Echo {
        msg_id: u64,
        echo: String,
    },
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
    }
}

fn main() {
    println!("Hello, world!");
}
