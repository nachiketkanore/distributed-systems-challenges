use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::Error::KeyDoesNotExist;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio_context::context::{Context, Handle};

#[derive(Clone)]
struct Handler {
    storage: Storage,
}

struct InnerData {
    data: HashMap<String, Vec<u64>>,
    committed_offsets: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    Send { key: String, msg: u64 },
    Poll { offsets: HashMap<String, usize> },
    CommitOffsets { offsets: HashMap<String, u64> },
    ListCommittedOffsets { keys: Vec<String> },
    Error { code: u64, text: String },
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
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: HashMap<String, u64>,
    },
}

impl Handler {
    /*
    messages are stored in this format:
    key1 : [msg1, msg2, ...]
           [   0,    1,   2... offsets for key1
    key2 : [msg1, msg2, ...]
           [   0,    1,   2... offsets for key2
    key3 : [msg1, msg2, ...]
           [   0,    1,   2... offsets for key3
    ...


    How do we store this data inside a key-value store?

    let's store the info as follows:

    - latest_offset_for_{key} -> some offset
    - value_for_{key}_at_offset_{offset} -> some value (message data)
    - committed_offset_for_key_{key} -> some offset

    */
    async fn add_message_to_key(&self, key: String, msg: u64, mut handler: Handle) -> usize {
        // TODO: store this start offset for each node in-memory
        // 1. Get the latest offset for given key from KV store
        // this helps use to minimize the search for the monotonous next offset to assign the given message
        let search_key = format!("latest_offset_for_{}", key);
        let mut start: usize = match self
            .storage
            .get(handler.spawn_ctx(), search_key.clone())
            .await
        {
            Ok(result) => result,
            Err(e) => 0,
        };

        // 2. Find the increasing offset to store the given message at
        //  - Loop over the latest offset retrieved from step 1
        //  - Success result on a CAS operation means (offset) can be used for storing the new message
        loop {
            let curr = start.clone();
            let (prev, now) = (curr.clone() - 1, curr);
            let res = self
                .storage
                .cas(handler.spawn_ctx(), search_key.clone(), prev, now, true)
                .await;
            match res {
                Ok(_) => break,
                Err(_) => {
                    start += 1;
                }
            }
        }

        // 3. Write the message at that corresponding offset location
        let messages_key = format!("value_for_{key}_at_offset_{start}");
        self.storage
            .put(handler.spawn_ctx(), messages_key, msg)
            .await
            .expect("Error occurred while writing to the KV store");

        // 4. Also update the latest offset for this key
        let search_key = format!("latest_offset_for_{}", key);

        let _ = self
            .storage
            .put(handler.spawn_ctx(), search_key, start)
            .await;

        start
    }

    async fn get_messages_for_offsets(
        &self,
        offsets: HashMap<String, usize>,
        mut handler: Handle,
    ) -> HashMap<String, Vec<Vec<u64>>> {
        // we limit the messages to send back to the caller
        const LIMIT: usize = 100;
        let mut msgs = HashMap::new();
        for (key, offset) in offsets {
            let mut response = Vec::new();
            for id in offset..(offset + LIMIT) {
                let search_key = format!("value_for_{key}_at_offset_{id}");
                let value_result: Result<u64> =
                    self.storage.get(handler.spawn_ctx(), search_key).await;
                match value_result {
                    Ok(value) => {
                        response.push(vec![id as u64, value as u64]);
                    }
                    Err(e) => {
                        continue;
                    }
                }
            }
            msgs.insert(key, response);
        }
        msgs
    }

    async fn update_offsets(&self, offsets: HashMap<String, u64>, mut handler: Handle) {
        for (key, offset) in offsets {
            let commit_key = format!("committed_offset_for_key_{key}");
            let result = self
                .storage
                .put(handler.spawn_ctx(), commit_key, offset)
                .await;
        }
    }

    async fn get_offsets(&self, keys: Vec<String>, mut handler: Handle) -> HashMap<String, u64> {
        let mut offsets = HashMap::new();

        for key in keys {
            let search_offsets_key = format!("committed_offset_for_key_{key}");
            let result: Result<u64> = self
                .storage
                .get(handler.spawn_ctx(), search_offsets_key)
                .await;
            let offset = match result {
                Ok(value) => value,
                Err(e) => 0,
            };
            offsets.insert(key, offset);
        }
        offsets
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let (_context, mut handler) = Context::new();
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Send { key, msg }) => {
                let offset = self.add_message_to_key(key, msg, handler).await;
                return runtime.reply(req, Response::SendOk { offset }).await;
            }
            Ok(Request::Poll { offsets }) => {
                let results = self.get_messages_for_offsets(offsets, handler).await;
                return runtime.reply(req, Response::PollOk { msgs: results }).await;
            }
            Ok(Request::CommitOffsets { offsets }) => {
                self.update_offsets(offsets, handler).await;
                return runtime.reply(req, Response::CommitOffsetsOk {}).await;
            }
            Ok(Request::ListCommittedOffsets { keys }) => {
                return runtime
                    .reply(
                        req,
                        Response::ListCommittedOffsetsOk {
                            offsets: self.get_offsets(keys, handler).await,
                        },
                    )
                    .await;
            }
            _ => done(runtime, req),
        }
    }
}

fn handler(runtime: Runtime) -> Handler {
    // this was for single node kafka challenge
    // we stored everything in-memory on the single server
    // let inner_data = InnerData {
    //     data: HashMap::new(),
    //     committed_offsets: HashMap::new(),
    // };
    // Handler { data: Arc::new(Mutex::new(inner_data)) }
    Handler {
        // storage: lin_kv(runtime),
        storage: seq_kv(runtime),
    }
}

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(handler(runtime.clone()));
    runtime.with_handler(handler).run().await
}
