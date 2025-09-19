# Hyperfile Reactor

A lightweight task execution framework built on top of Tokio's LocalSet.

## Overview

Hyperfile Reactor enables spawning tasks that can handle concurrent requests through multiple priority channels within single thread.

The implementation is based on [Tokio's LocalSet example](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html#use-inside-tokiospawn).

## License

This project is licensed under the Apache-2.0 License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
