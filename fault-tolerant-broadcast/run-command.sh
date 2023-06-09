# this solution fails - incorrect passed earlier
cargo build --release
~/maelstrom/maelstrom/maelstrom test -w broadcast --bin ~/distributed-systems-challenges/fault-tolerant-broadcast/target/release/fault-tolerant-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
