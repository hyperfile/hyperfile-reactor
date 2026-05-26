# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-1.0, minor versions may include breaking changes.

## [0.3.0] - 2026-05-26

This is a major redesign of the public API. Almost everything changed; see
the migration notes below.

### Added

- `Reactor<Ctx, T>`: owns a dedicated background thread driving a tokio
  `current_thread` runtime + `LocalSet`. Replaces `LocalSpawner`.
  - `Reactor::new_current` / `Reactor::new(rt)` constructors.
  - `Reactor::spawn` / `Reactor::spawn_with` (blocking) and
    `Reactor::spawn_async` / `Reactor::spawn_with_async` (use from inside a
    tokio runtime).
  - `Reactor::shutdown` for explicit close + join; `Drop` joins
    best-effort.
- `TaskBuilder<Ctx>`: register an arbitrary number of channels, each with a
  user-chosen `u8` priority (0 = highest) and `Capacity` (`Bounded(n)` /
  `Unbounded`). Registration returns a typed `Channel<Ctx>` token.
  - `add_channel(priority, capacity)`, `add_unbounded(priority)`,
    `add_bounded(priority, n)`.
- `Channel<Ctx>`: opaque, `Copy` token used to select a destination
  channel when sending.
- `TaskHandler<Ctx>` send variants:
  - `try_send` / `send`: non-blocking; returns
    `TrySendError::Full(ctx)` when a bounded channel is full.
  - `send_async`: awaits free capacity on bounded channels.
- `Scheduler<Ctx>` trait: pluggable dispatch policy. Two built-in
  implementations:
  - `StrictPriority`: always serves the highest-priority non-empty
    channel.
  - `FairPriority::new(budget)` (default `budget = 16`): strict priority
    with anti-starvation. After `budget` consecutive top-priority items,
    one item from the lowest non-empty band is served.
- `SendError::into_inner` and `TrySendError::into_inner` to recover the
  un-sent context.
- `examples/basic.rs` showcasing bounded + unbounded channels, scheduler
  selection via `spawn_with`, and `send_async` backpressure.

### Changed

- **Renamed** `LocalSpawner` → `Reactor`.
- **`Task` trait** is now business-logic-only:
  - Old: `Task::handler` returned an `impl Future`, plus a `Task::start`
    method that hard-coded channel and loop construction.
  - New: a single `Task::handle(&mut self, ctx)` method. All startup
    logic moved into `Reactor::spawn`.
- **Priority dispatch** is now correct. The previous implementation used
  `futures_lite::future::or` to combine three hard-coded receivers, which
  did not actually enforce strict priority. The new schedulers do
  non-blocking `try_recv` scans in priority order and use a `poll_fn`-based
  biased await for fairness.
- **`send` no longer panics on closed receiver.** Producer methods return
  a `Result`. The previous behaviour of panicking made it impossible for
  callers to recover after a task had stopped.
- The minimum supported Rust version is now `1.75` (declared in
  `Cargo.toml` as `rust-version = "1.75"`).

### Removed

- `LocalSpawner` (use `Reactor`).
- The hard-coded three-channel layout (`tx` / `highprio_tx` / `cb_tx`) and
  the corresponding `TaskHandler::send` / `send_highprio` / `send_cb`
  methods. Channels are now user-defined.
- The `futures-lite` dependency.

### Internal

- Public enums `Capacity`, `TrySendError`, and `DynReceiver` are marked
  `#[non_exhaustive]` so that adding variants in future releases is not a
  breaking change.
- `tokio` features minimized to `rt`, `sync`, `macros`, `time`.
- `cargo fmt` and `cargo clippy --all-targets -- -D warnings` are clean.
- `#![warn(missing_docs)]` enabled at crate level; every public item is
  documented.

### Migration: 0.2.x → 0.3.0

The 0.2 API exposed exactly three hard-coded mpsc channels per task
(`send`, `send_highprio`, `send_cb`). The 0.3 API lets you declare
whatever channel topology you need.

```rust
// 0.2
use hyperfile_reactor::{LocalSpawner, Task};
use tokio::sync::oneshot;

impl Task<MyCtx> for MyTask {
    async fn handler(&mut self, ctx: MyCtx) { /* ... */ }
}

let spawner = LocalSpawner::<MyCtx, MyTask>::new_current();
let (tx, rx) = oneshot::channel();
spawner.spawn(MyTask, tx);
let handler = rx.blocking_recv().unwrap();

handler.send(ctx_normal);          // priority 2
handler.send_highprio(ctx_high);   // priority 1
handler.send_cb(ctx_critical);     // priority 0
```

```rust
// 0.3
use hyperfile_reactor::{Reactor, Task, TaskBuilder};

impl Task<MyCtx> for MyTask {
    async fn handle(&mut self, ctx: MyCtx) { /* ... */ }
}

let reactor = Reactor::<MyCtx, MyTask>::new_current().unwrap();

let mut builder = TaskBuilder::<MyCtx>::new();
let critical = builder.add_unbounded(0); // 0 = highest priority
let high     = builder.add_unbounded(1);
let normal   = builder.add_unbounded(2);

let handler = reactor.spawn(MyTask, builder).unwrap();

handler.send(normal,   ctx_normal).unwrap();
handler.send(high,     ctx_high).unwrap();
handler.send(critical, ctx_critical).unwrap();

drop(handler);
reactor.shutdown().unwrap();
```

Notable behavioural differences to be aware of:

- Priority is now actually enforced (this was a bug in 0.2).
- `send` no longer panics; check the returned `Result`.
- The reactor thread is owned: call `reactor.shutdown()` (or rely on
  `Drop`) instead of leaking the thread.
- For backpressure, register channels with `add_bounded(prio, n)` and
  produce via `handler.send_async(...)`.

[0.3.0]: https://github.com/hyperfile/hyperfile-reactor/releases/tag/v0.3.0
