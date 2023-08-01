# build the release binary
cargo build --release --bin multi-kafka

~/maelstrom/maelstrom/maelstrom test -w kafka --bin ./target/release/multi-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
