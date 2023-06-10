cargo build --release
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/efficient-broadcast-part2/target/release/efficient-broadcast-part2 --node-count 25 --time-limit 20 --rate 100 --latency 100
