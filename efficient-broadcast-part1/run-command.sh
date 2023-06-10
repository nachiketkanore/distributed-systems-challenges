cargo build --release
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/fault-tolerant-broadcast/target/release/fault-tolerant-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
