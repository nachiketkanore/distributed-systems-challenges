cargo build --release

~/maelstrom/maelstrom/maelstrom test -w g-counter --bin ~/distributed-systems-challenges/grow-only-counter/target/release/grow-only-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
