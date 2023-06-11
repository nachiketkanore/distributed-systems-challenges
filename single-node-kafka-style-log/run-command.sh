# build the release binary
cargo build --release

~/maelstrom/maelstrom/maelstrom test -w kafka --bin ~/distributed-systems-challenges/single-node-kafka-style-log/target/release/single-node-kafka-style-log --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
