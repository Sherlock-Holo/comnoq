# Code style

1. Use Rust Edition 2024 style.
2. Never use `Arc::clone(&xxx)` style, instead use `xxx.clone()`.