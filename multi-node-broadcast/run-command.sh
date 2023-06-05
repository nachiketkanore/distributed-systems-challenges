# create binary
cargo build --release

# run maelstrom against our binary
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/multi-node-broadcast/target/release/multi-node-broadcast --node-count 5 --time-limit 20 --rate 10
