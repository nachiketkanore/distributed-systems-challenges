# create binary
cargo build

# run maelstrom against our binary
~/maelstrom/maelstrom/maelstrom test -w echo --bin ~/distributed-systems-challenges/echo/target/debug/echo --node-count 1 --time-limit 10
