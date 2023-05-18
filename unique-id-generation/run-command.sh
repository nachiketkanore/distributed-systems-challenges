# build our binary from rust project
cargo build

# run maelstrom test against our binary
~/maelstrom/maelstrom/maelstrom test -w unique-ids --bin ~/distributed-systems-challenges/unique-id-generation/target/debug/unique-id-generation --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition