cargo build --release
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/efficient-broadcast-part1/target/release/efficient-broadcast-part1 --node-count 25 --time-limit 20 --rate 100 --latency 100
