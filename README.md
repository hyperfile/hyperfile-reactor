# Hyperfile Reactor

[![crates.io](https://img.shields.io/crates/v/hyperfile-reactor.svg)](https://crates.io/crates/hyperfile-reactor)
[![docs.rs](https://docs.rs/hyperfile-reactor/badge.svg)](https://docs.rs/hyperfile-reactor)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

A lightweight task execution framework built on top of Tokio's
[`LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html).

A `Reactor` owns a dedicated OS thread running a `current_thread` tokio
runtime plus a `LocalSet`. You register **typed tasks** on the reactor; each
task listens on one or more **user-defined channels**, each with its own
**priority** and **capacity**, and processes incoming contexts via a
single-threaded handler. Channel dispatch is governed by a pluggable
[`Scheduler`].

## When to use

- You need single-threaded handlers (e.g. owning `!Send` state, a file
  handle, an in-memory cache, a state machine) but still want to fan in
  work from many producer threads.
- You want priority-aware dispatch with optional anti-starvation, without
  rolling your own `select_biased!` loop.
- You want backpressure on bounded channels, without giving up the option
  of unbounded ones for low-volume control paths.

## When **not** to use

- Your handler does CPU-heavy or blocking work — that will stall every
  channel on the reactor thread. Offload such work via `tokio::task::spawn`,
  `spawn_blocking`, or a separate worker pool.
- You need multi-consumer dispatch (worker pool semantics). This crate is
  one consumer per task by design.

## Quickstart

`Cargo.toml`:

```toml
[dependencies]
hyperfile-reactor = "0.3"
tokio = { version = "1", features = ["rt", "sync", "macros", "time"] }
```

```rust
use hyperfile_reactor::{Reactor, Task, TaskBuilder};

struct Echo;

impl Task<String> for Echo {
    async fn handle(&mut self, msg: String) {
        println!("got: {msg}");
    }
}

fn main() {
    let reactor = Reactor::<String, Echo>::new_current().unwrap();

    // Register channels: lower priority value = higher priority (0 = highest).
    let mut builder = TaskBuilder::<String>::new();
    let high = builder.add_unbounded(0);
    let low  = builder.add_unbounded(9);

    // Spawn the task; returns a cloneable producer handle.
    let handler = reactor.spawn(Echo, builder).unwrap();

    handler.send(high, "urgent".into()).unwrap();
    handler.send(low,  "later".into()).unwrap();

    drop(handler);              // close the channels
    reactor.shutdown().unwrap(); // join the background thread
}
```

## Channels and backpressure

Each channel registered on a `TaskBuilder` is independent and has its own
[`Capacity`]:

```rust
use hyperfile_reactor::{Capacity, TaskBuilder};

let mut builder = TaskBuilder::<MyCtx>::new();

// Bounded: producers experience backpressure when the queue is full.
let critical = builder.add_bounded(0, 64);

// Unbounded: producers never block, at the cost of no memory safety net.
let bulk = builder.add_unbounded(9);

// Or use the explicit form:
let other = builder.add_channel(5, Capacity::Bounded(256));
```

Producers pick the right send method based on what they need:

| Method | Behaviour | Returns |
| --- | --- | --- |
| `try_send` / `send` | Non-blocking. On a full bounded channel, returns `Err(TrySendError::Full(ctx))`. | `Result<(), TrySendError<Ctx>>` |
| `send_async` | On a bounded channel, awaits free capacity. | `Result<(), SendError<Ctx>>` |

`send` and `try_send` are the same call (both non-blocking). `send_async`
is the only way to apply backpressure on a bounded channel without
discarding work.

## Schedulers

The default scheduler is [`FairPriority`]: strict priority with
anti-starvation. After a configurable budget of consecutive top-priority
items, one item from the lowest non-empty priority band is served.

Pick a different scheduler with `spawn_with`:

```rust
use hyperfile_reactor::{FairPriority, Reactor, StrictPriority};

// Strict priority: low-priority work may starve under sustained high-prio load.
let handler = reactor
    .spawn_with(task, builder, StrictPriority)
    .unwrap();

// Fair priority with custom budget.
let handler = reactor
    .spawn_with(task, builder, FairPriority::new(4))
    .unwrap();
```

Built-ins:

- [`StrictPriority`] — always serves the highest-priority non-empty channel.
- [`FairPriority::new(budget)`] — strict priority with anti-starvation.
  `FairPriority::default()` uses `budget = 16`.

You can implement your own [`Scheduler`] if you need weighted round-robin,
token-bucket, deadline-based, or any other policy.

## Spawning from inside a tokio runtime

`Reactor::spawn` blocks the current thread until the task starts. **Don't
call it from inside an existing tokio runtime** — use the `_async`
variants:

```rust
let handler = reactor.spawn_async(task, builder).await.unwrap();
let handler = reactor
    .spawn_with_async(task, builder, StrictPriority)
    .await
    .unwrap();
```

## Shutdown

A `Reactor` owns its background thread. The thread joins when:

1. all `TaskHandler` clones for every spawned task have been dropped, **and**
2. `Reactor::shutdown()` is called or the `Reactor` is dropped.

Each task's scheduler loop exits once all its senders are gone. If you call
`shutdown()` while producers are still alive, it will block waiting for
them. Drop the handlers first:

```rust
drop(handler);                      // release this task's senders
reactor.shutdown().unwrap();        // join the background thread
```

## Handler discipline

The handler runs single-threaded inside a `LocalSet`. Two rules:

1. **Never block.** No `std::thread::sleep`, no synchronous IO, no busy
   loops. Use `tokio::time::sleep`, async IO, or hand work to
   `tokio::task::spawn` / `spawn_blocking`.
2. **Be fast in the synchronous portion.** Anything you `await` on inside
   `handle` pauses the entire reactor for this task. Long-running async
   work should usually be detached via `tokio::task::spawn`.

## Example

A runnable example demonstrating bounded channels, scheduler choice, and
`send_async` backpressure:

```bash
cargo run --example basic
```

## Stability

Pre-1.0; minor versions may include breaking API changes. Public enums are
`#[non_exhaustive]` to keep adding variants from being a breaking change.

## License

Apache-2.0. See [LICENSE](LICENSE).

## Contributing

Issues and pull requests are welcome.
