# create binary
cargo build

# run maelstrom against our binary
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/single-node-broadcast/target/debug/single-node-broadcast --node-count 1 --time-limit 20 --rate 10
