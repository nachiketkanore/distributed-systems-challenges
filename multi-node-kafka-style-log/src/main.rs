use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
// use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio_context::context::Context;

#[derive(Clone)]
struct Handler {
    data: Arc<Mutex<InnerData>>,
    // s: Storage,
}

struct InnerData {
    data: HashMap<String, Vec<u64>>,
    committed_offsets: HashMap<String, u64>
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    Send {
        key: String,
        msg: u64,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    CommitOffsets {
        offsets: HashMap<String, u64>,
    },
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    Error {
        code: u64,
        text: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Response {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<u64>>>,
    },
    CommitOffsetsOk {
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    }
}

impl Handler {
    fn add_message_to_key(&self, key: String, msg: u64) -> usize {
        let mut state = self.data.lock().unwrap();

        let idx = if state.data.contains_key(&key) {
            let x = state.data.entry(key).or_insert(vec![msg]);
            x.push(msg);
            x.len()
        } else {
            state.data.insert(key, vec![msg]);
            1
        };
        return idx - 1;
        // no need, handled automatically
        // drop(state);
    }

    fn get_messages_for_offsets(&self, offsets: HashMap<String, usize>) ->  HashMap<String, Vec<Vec<u64>>> {
        const LIMIT: usize = 20;
        let mut msgs = HashMap::new();
        let state = self.data.lock().unwrap();

        for (key, offset) in offsets {
            if let Some(values) = state.data.get(&key) {
                let mut response = Vec::new();
                for id in offset..values.len().min(offset + LIMIT) {
                    response.push(vec![id as u64, values[id]]);
                }
                msgs.insert(key, response);
            }
        }
        msgs
    }

    fn update_offsets(&self, offsets: HashMap<String, u64>) {
        let mut state = self.data.lock().unwrap();
        for (key, offset) in offsets {
            state.committed_offsets
                .entry(key)
                .and_modify(|curr| *curr = (*curr).max(offset))
                .or_insert(offset);
        }
        // work successful
    }

    fn get_offsets(&self, keys: Vec<String>) -> HashMap<String, u64> {
        let state = self.data.lock().unwrap();
        let mut offsets = HashMap::new();
        for key in keys {
            if let Some(&val) = state.committed_offsets.get(&key) {
                offsets.insert(key, val);
            }
        }
        offsets
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let (_ctx, _handler) = Context::new();
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Send { key, msg}) => {
                let offset = self.add_message_to_key(key, msg);
                return runtime.reply(req, Response::SendOk { offset }).await;
            },
            Ok(Request::Poll { offsets }) => {
                let results = self.get_messages_for_offsets(offsets);
                return runtime.reply(req, Response::PollOk { msgs: results }).await;
            },
            Ok(Request::CommitOffsets { offsets }) => {
                self.update_offsets(offsets);
                return runtime.reply(req, Response::CommitOffsetsOk {}).await;
                // return runtime.reply_ok().await;
            },
            Ok(Request::ListCommittedOffsets { keys }) => {
                return runtime.reply(req, Response::ListCommittedOffsetsOk {
                    offsets: self.get_offsets(keys)
                }).await;
            },
            _ => done(runtime, req),
        }
    }
}

fn handler(_runtime: Runtime) -> Handler {
    let inner_data = InnerData {
        data: HashMap::new(),
        committed_offsets: HashMap::new(),
    };
    Handler { data: Arc::new(Mutex::new(inner_data)) }
    // Handler { s: lin_kv(runtime) }
}

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(handler(runtime.clone()));
    runtime.with_handler(handler).run().await
}
