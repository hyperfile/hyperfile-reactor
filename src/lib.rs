//! A lightweight task execution framework built on top of Tokio's `LocalSet`.
//!
//! # Overview
//!
//! A [`Reactor`] owns a dedicated OS thread running a tokio `current_thread`
//! runtime plus a [`LocalSet`]. Users register their own **typed tasks** on
//! the reactor; each task listens on one or more **user-defined channels**,
//! each with its own **priority** and **capacity**, and processes incoming
//! contexts through a user-supplied handler. The dispatch policy is
//! pluggable via the [`Scheduler`] trait; [`FairPriority`] (the default) and
//! [`StrictPriority`] are provided.
//!
//! ```no_run
//! use hyperfile_reactor::{Capacity, Reactor, Task, TaskBuilder};
//!
//! struct MyTask;
//! impl Task<u64> for MyTask {
//!     async fn handle(&mut self, ctx: u64) {
//!         println!("got {ctx}");
//!     }
//! }
//!
//! let reactor = Reactor::<u64, MyTask>::new_current().unwrap();
//!
//! let mut builder = TaskBuilder::<u64>::new();
//! let high = builder.add_channel(0, Capacity::Bounded(64));  // backpressured
//! let low  = builder.add_channel(9, Capacity::Unbounded);
//!
//! let handler = reactor.spawn(MyTask, builder).expect("spawn");
//! handler.send(high, 1).unwrap();
//! handler.send(low,  2).unwrap();
//! ```

use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;

use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Returned when a send fails because the task has stopped.
#[derive(Debug)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task receiver closed")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Returned from non-blocking send paths: either the bounded channel is full
/// or the task has stopped.
///
/// This enum is `#[non_exhaustive]`; callers must include a wildcard arm
/// when matching to remain forward-compatible.
#[derive(Debug)]
#[non_exhaustive]
pub enum TrySendError<T> {
    /// Bounded channel currently full. The item is returned to the caller.
    Full(T),
    /// Receiver has been dropped / task has stopped.
    Closed(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(t) | Self::Closed(t) => t,
        }
    }
}

impl<T> std::fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.write_str("channel full"),
            Self::Closed(_) => f.write_str("task receiver closed"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for TrySendError<T> {}

/// Returned when spawning a task fails because the reactor thread is gone.
#[derive(Debug)]
pub struct SpawnError;

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("reactor background thread has shut down")
    }
}

impl std::error::Error for SpawnError {}

// ---------------------------------------------------------------------------
// Task trait (business logic only)
// ---------------------------------------------------------------------------

/// Business-logic trait. Implemented by users for each task type.
///
/// The handler runs inside a single-threaded runtime. **Never block it.**
/// Offload blocking or long-running IO via `tokio::task::spawn`,
/// `spawn_blocking`, or similar.
pub trait Task<Ctx>: 'static {
    fn handle(&mut self, ctx: Ctx) -> impl std::future::Future<Output = ()>;
}

// ---------------------------------------------------------------------------
// Capacity + DynSender / DynReceiver
// ---------------------------------------------------------------------------

/// Channel capacity policy.
///
/// `#[non_exhaustive]` so that adding new policies (e.g. bounded with an
/// overflow strategy) in a future release is not a breaking change.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Capacity {
    /// Bounded channel; producers apply backpressure when full.
    Bounded(usize),
    /// Unbounded channel; producers never block, but producers can OOM the
    /// consumer if they vastly outpace it.
    Unbounded,
}

/// Internal unified sender.
pub(crate) enum DynSender<T> {
    Bounded(mpsc::Sender<T>),
    Unbounded(mpsc::UnboundedSender<T>),
}

impl<T> Clone for DynSender<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Bounded(s) => Self::Bounded(s.clone()),
            Self::Unbounded(s) => Self::Unbounded(s.clone()),
        }
    }
}

impl<T> DynSender<T> {
    /// Non-blocking send. For bounded channels, returns `Full` if the queue
    /// is full.
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self {
            Self::Bounded(s) => s.try_send(value).map_err(|e| match e {
                mpsc::error::TrySendError::Full(v) => TrySendError::Full(v),
                mpsc::error::TrySendError::Closed(v) => TrySendError::Closed(v),
            }),
            Self::Unbounded(s) => s.send(value).map_err(|e| TrySendError::Closed(e.0)),
        }
    }

    /// Async send. Unbounded channels resolve immediately; bounded channels
    /// await capacity.
    async fn send_async(&self, value: T) -> Result<(), SendError<T>> {
        match self {
            Self::Bounded(s) => s.send(value).await.map_err(|e| SendError(e.0)),
            Self::Unbounded(s) => s.send(value).map_err(|e| SendError(e.0)),
        }
    }
}

/// Internal unified receiver used by [`Scheduler`] implementations.
///
/// User code should treat this as opaque: only call [`try_recv`](Self::try_recv)
/// or [`poll_recv`](Self::poll_recv). Both the enum and its variants are
/// `#[non_exhaustive]`; new representations may be added without a major
/// version bump, and external code may not construct or destructure them.
#[non_exhaustive]
pub enum DynReceiver<T> {
    #[non_exhaustive]
    Bounded(mpsc::Receiver<T>),
    #[non_exhaustive]
    Unbounded(mpsc::UnboundedReceiver<T>),
}

impl<T> DynReceiver<T> {
    /// Non-blocking receive. Returns `Err(TryRecvError::Empty)` if no value
    /// is ready; `Err(TryRecvError::Disconnected)` if all senders are gone.
    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        match self {
            Self::Bounded(r) => r.try_recv(),
            Self::Unbounded(r) => r.try_recv(),
        }
    }

    /// Poll-based receive; mirrors `Receiver::poll_recv`.
    pub fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        match self {
            Self::Bounded(r) => r.poll_recv(cx),
            Self::Unbounded(r) => r.poll_recv(cx),
        }
    }
}

fn make_channel<T>(cap: Capacity) -> (DynSender<T>, DynReceiver<T>) {
    match cap {
        Capacity::Bounded(n) => {
            let (tx, rx) = mpsc::channel(n.max(1));
            (DynSender::Bounded(tx), DynReceiver::Bounded(rx))
        }
        Capacity::Unbounded => {
            let (tx, rx) = mpsc::unbounded_channel();
            (DynSender::Unbounded(tx), DynReceiver::Unbounded(rx))
        }
    }
}

// ---------------------------------------------------------------------------
// Channel token + TaskBuilder
// ---------------------------------------------------------------------------

/// An opaque token identifying one channel registered on a task.
pub struct Channel<Ctx> {
    idx: usize,
    _marker: PhantomData<fn(Ctx)>,
}

impl<Ctx> Clone for Channel<Ctx> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Ctx> Copy for Channel<Ctx> {}

impl<Ctx> std::fmt::Debug for Channel<Ctx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel").field("idx", &self.idx).finish()
    }
}

#[derive(Clone, Copy, Debug)]
struct ChannelSpec {
    /// Lower value = higher priority. 0 is the highest.
    priority: u8,
    capacity: Capacity,
}

/// Declares the set of channels a task will receive on.
pub struct TaskBuilder<Ctx> {
    channels: Vec<ChannelSpec>,
    _marker: PhantomData<fn(Ctx)>,
}

impl<Ctx> Default for TaskBuilder<Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Ctx> TaskBuilder<Ctx> {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Register a channel with the given priority (0 = highest) and capacity.
    ///
    /// Multiple channels may share the same priority; among equals the
    /// default scheduler visits them in registration order.
    pub fn add_channel(&mut self, priority: u8, capacity: Capacity) -> Channel<Ctx> {
        let idx = self.channels.len();
        self.channels.push(ChannelSpec { priority, capacity });
        Channel {
            idx,
            _marker: PhantomData,
        }
    }

    /// Convenience: register an unbounded channel.
    pub fn add_unbounded(&mut self, priority: u8) -> Channel<Ctx> {
        self.add_channel(priority, Capacity::Unbounded)
    }

    /// Convenience: register a bounded channel with the given capacity.
    pub fn add_bounded(&mut self, priority: u8, capacity: usize) -> Channel<Ctx> {
        self.add_channel(priority, Capacity::Bounded(capacity))
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

// ---------------------------------------------------------------------------
// TaskHandler (producer side)
// ---------------------------------------------------------------------------

/// Producer-side handle. Cheap to clone; can be shared across threads.
pub struct TaskHandler<Ctx> {
    senders: Arc<[DynSender<Ctx>]>,
}

impl<Ctx> Clone for TaskHandler<Ctx> {
    fn clone(&self) -> Self {
        Self {
            senders: self.senders.clone(),
        }
    }
}

impl<Ctx> TaskHandler<Ctx> {
    /// Non-blocking send. On a bounded channel this returns `Full` when the
    /// queue is at capacity; on an unbounded channel it is effectively
    /// infallible except when the task has already stopped.
    pub fn try_send(&self, ch: Channel<Ctx>, ctx: Ctx) -> Result<(), TrySendError<Ctx>> {
        self.senders[ch.idx].try_send(ctx)
    }

    /// Convenience wrapper over [`try_send`](Self::try_send) that flattens
    /// `Full` back into `Closed`-style errors. Use [`send_async`](Self::send_async)
    /// when you want proper backpressure on bounded channels.
    ///
    /// For unbounded channels this is equivalent to `try_send`.
    pub fn send(&self, ch: Channel<Ctx>, ctx: Ctx) -> Result<(), TrySendError<Ctx>> {
        self.try_send(ch, ctx)
    }

    /// Async send with backpressure. On bounded channels this awaits free
    /// capacity; on unbounded channels it resolves immediately.
    pub async fn send_async(
        &self,
        ch: Channel<Ctx>,
        ctx: Ctx,
    ) -> Result<(), SendError<Ctx>> {
        self.senders[ch.idx].send_async(ctx).await
    }

    pub fn channel_count(&self) -> usize {
        self.senders.len()
    }
}

// ---------------------------------------------------------------------------
// Scheduler trait + built-in implementations
// ---------------------------------------------------------------------------

/// Pluggable dispatch policy.
///
/// Given the task's receivers and their priorities (parallel arrays, aligned
/// by channel index), a scheduler decides which context to process next.
/// Return `None` only when every receiver is closed and the task should
/// terminate.
pub trait Scheduler<Ctx>: Send + 'static {
    fn next_ctx<'a>(
        &'a mut self,
        rxs: &'a mut [DynReceiver<Ctx>],
        priorities: &'a [u8],
    ) -> impl std::future::Future<Output = Option<Ctx>> + 'a;
}

/// Strict priority: always serve the highest-priority non-empty channel.
/// Lower-priority channels may starve if higher ones never drain.
#[derive(Clone, Copy, Debug, Default)]
pub struct StrictPriority;

impl<Ctx> Scheduler<Ctx> for StrictPriority {
    async fn next_ctx<'a>(
        &'a mut self,
        rxs: &'a mut [DynReceiver<Ctx>],
        priorities: &'a [u8],
    ) -> Option<Ctx> {
        let order = sort_order(priorities);

        // Non-blocking scan.
        if let Some(v) = try_recv_in_order(rxs, &order) {
            return Some(v);
        }
        // Block until something arrives.
        await_any(rxs, &order).await.map(|(v, _)| v)
    }
}

/// Strict priority with anti-starvation: after `budget` consecutive items
/// from the top-priority band, serve one item from the lowest non-empty band
/// if any is waiting.
#[derive(Clone, Copy, Debug)]
pub struct FairPriority {
    pub budget: u32,
    top_streak: u32,
}

impl FairPriority {
    pub fn new(budget: u32) -> Self {
        Self {
            budget,
            top_streak: 0,
        }
    }
}

impl Default for FairPriority {
    fn default() -> Self {
        Self::new(16)
    }
}

impl<Ctx> Scheduler<Ctx> for FairPriority {
    async fn next_ctx<'a>(
        &'a mut self,
        rxs: &'a mut [DynReceiver<Ctx>],
        priorities: &'a [u8],
    ) -> Option<Ctx> {
        let order = sort_order(priorities);
        if order.is_empty() {
            return None;
        }
        let top_idx = order[0];

        // 1) Starvation relief.
        if self.top_streak >= self.budget {
            self.top_streak = 0;
            if let Some(v) = try_recv_lowest_nonempty(rxs, &order) {
                return Some(v);
            }
            // nothing below waiting; fall through
        }

        // 2) Non-blocking priority scan.
        if let Some((v, idx)) = try_recv_in_order_tagged(rxs, &order) {
            self.top_streak = if idx == top_idx {
                self.top_streak.saturating_add(1)
            } else {
                0
            };
            return Some(v);
        }

        // 3) Await any; track top-streak.
        match await_any(rxs, &order).await {
            Some((v, is_top)) => {
                self.top_streak = if is_top {
                    self.top_streak.saturating_add(1)
                } else {
                    0
                };
                Some(v)
            }
            None => None,
        }
    }
}

// --- shared scheduler helpers ---

/// Return indices 0..N sorted by ascending priority value (stable).
fn sort_order(priorities: &[u8]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..priorities.len()).collect();
    order.sort_by_key(|&i| priorities[i]);
    order
}

fn try_recv_in_order<Ctx>(
    rxs: &mut [DynReceiver<Ctx>],
    order: &[usize],
) -> Option<Ctx> {
    for &idx in order {
        if let Ok(v) = rxs[idx].try_recv() {
            return Some(v);
        }
    }
    None
}

fn try_recv_in_order_tagged<Ctx>(
    rxs: &mut [DynReceiver<Ctx>],
    order: &[usize],
) -> Option<(Ctx, usize)> {
    for &idx in order {
        if let Ok(v) = rxs[idx].try_recv() {
            return Some((v, idx));
        }
    }
    None
}

fn try_recv_lowest_nonempty<Ctx>(
    rxs: &mut [DynReceiver<Ctx>],
    order: &[usize],
) -> Option<Ctx> {
    for &idx in order.iter().rev() {
        if let Ok(v) = rxs[idx].try_recv() {
            return Some(v);
        }
    }
    None
}

async fn await_any<Ctx>(
    rxs: &mut [DynReceiver<Ctx>],
    order: &[usize],
) -> Option<(Ctx, bool)> {
    use std::future::poll_fn;
    use std::task::Poll;

    if order.is_empty() {
        return None;
    }
    let top = order[0];

    poll_fn(|cx| {
        let mut all_closed = true;
        for &idx in order {
            match rxs[idx].poll_recv(cx) {
                Poll::Ready(Some(v)) => return Poll::Ready(Some((v, idx == top))),
                Poll::Ready(None) => { /* closed; keep scanning */ }
                Poll::Pending => all_closed = false,
            }
        }
        if all_closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Reactor (owns the background thread)
// ---------------------------------------------------------------------------

type SpawnRequest = Box<dyn FnOnce() + Send>;

/// Owns the background thread running the tokio runtime + `LocalSet`.
///
/// # Shutdown
///
/// Dropping the reactor (or calling [`shutdown`](Self::shutdown)) closes the
/// spawn control channel and joins the background thread. **The background
/// thread only exits once every [`TaskHandler`] clone has been dropped.**
/// If producers are still alive, `Drop`/`shutdown` will block.
pub struct Reactor<Ctx, T> {
    ctrl: Option<mpsc::UnboundedSender<SpawnRequest>>,
    thread: Option<JoinHandle<()>>,
    _marker: PhantomData<fn(Ctx, T)>,
}

impl<Ctx, T> Reactor<Ctx, T>
where
    Ctx: Send + 'static,
    T: Task<Ctx> + Send + 'static,
{
    /// Build a reactor on a freshly created `current_thread` runtime.
    pub fn new_current() -> std::io::Result<Self> {
        Self::new(None)
    }

    /// Build a reactor. Supply `Some(rt)` to reuse an existing runtime.
    pub fn new(runtime: Option<Arc<Runtime>>) -> std::io::Result<Self> {
        let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel::<SpawnRequest>();

        let rt = match runtime {
            Some(r) => r,
            None => Arc::new(Builder::new_current_thread().enable_all().build()?),
        };

        let thread = std::thread::Builder::new()
            .name("hyperfile-reactor".into())
            .spawn(move || {
                let local = LocalSet::new();

                local.spawn_local(async move {
                    while let Some(req) = ctrl_rx.recv().await {
                        req();
                    }
                });

                rt.block_on(local);
            })?;

        Ok(Self {
            ctrl: Some(ctrl_tx),
            thread: Some(thread),
            _marker: PhantomData,
        })
    }

    /// Spawn a task using the default scheduler ([`FairPriority`] with
    /// budget 16).
    ///
    /// Blocks the current thread until the task has been started. **Do not
    /// call this from inside a tokio runtime**; use [`spawn_async`](Self::spawn_async)
    /// instead.
    pub fn spawn(
        &self,
        task: T,
        builder: TaskBuilder<Ctx>,
    ) -> Result<TaskHandler<Ctx>, SpawnError> {
        self.spawn_with(task, builder, FairPriority::default())
    }

    /// Spawn a task with a user-supplied scheduler (blocking).
    pub fn spawn_with<S>(
        &self,
        task: T,
        builder: TaskBuilder<Ctx>,
        scheduler: S,
    ) -> Result<TaskHandler<Ctx>, SpawnError>
    where
        S: Scheduler<Ctx>,
    {
        let rx = self.submit(task, builder, scheduler)?;
        rx.blocking_recv().map_err(|_| SpawnError)
    }

    /// Async version of [`spawn`](Self::spawn). Use this from inside a
    /// tokio runtime.
    pub async fn spawn_async(
        &self,
        task: T,
        builder: TaskBuilder<Ctx>,
    ) -> Result<TaskHandler<Ctx>, SpawnError> {
        self.spawn_with_async(task, builder, FairPriority::default())
            .await
    }

    /// Async version of [`spawn_with`](Self::spawn_with).
    pub async fn spawn_with_async<S>(
        &self,
        task: T,
        builder: TaskBuilder<Ctx>,
        scheduler: S,
    ) -> Result<TaskHandler<Ctx>, SpawnError>
    where
        S: Scheduler<Ctx>,
    {
        let rx = self.submit(task, builder, scheduler)?;
        rx.await.map_err(|_| SpawnError)
    }

    /// Shared internals: build the spawn request, push it onto the reactor
    /// control channel, return the oneshot receiver.
    fn submit<S>(
        &self,
        task: T,
        builder: TaskBuilder<Ctx>,
        scheduler: S,
    ) -> Result<oneshot::Receiver<TaskHandler<Ctx>>, SpawnError>
    where
        S: Scheduler<Ctx>,
    {
        let ctrl = self.ctrl.as_ref().ok_or(SpawnError)?;
        let (tx, rx) = oneshot::channel::<TaskHandler<Ctx>>();

        let req: SpawnRequest = Box::new(move || {
            let handler = start_task(task, builder, scheduler);
            let _ = tx.send(handler);
        });

        ctrl.send(req).map_err(|_| SpawnError)?;
        Ok(rx)
    }

    /// Close the control channel and join the background thread.
    pub fn shutdown(mut self) -> std::thread::Result<()> {
        drop(self.ctrl.take());
        if let Some(h) = self.thread.take() {
            h.join()
        } else {
            Ok(())
        }
    }
}

impl<Ctx, T> Drop for Reactor<Ctx, T> {
    fn drop(&mut self) {
        drop(self.ctrl.take());
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// start_task: create channels + spawn scheduler loop
// ---------------------------------------------------------------------------

fn start_task<Ctx, T, S>(
    mut task: T,
    builder: TaskBuilder<Ctx>,
    mut scheduler: S,
) -> TaskHandler<Ctx>
where
    Ctx: Send + 'static,
    T: Task<Ctx> + 'static,
    S: Scheduler<Ctx>,
{
    let cap_hint = builder.channels.len().max(1);
    let mut senders: Vec<DynSender<Ctx>> = Vec::with_capacity(cap_hint);
    let mut receivers: Vec<DynReceiver<Ctx>> = Vec::with_capacity(cap_hint);
    let mut priorities: Vec<u8> = Vec::with_capacity(cap_hint);

    if builder.channels.is_empty() {
        // Degenerate case: create a single unbounded channel so the handler
        // is usable.
        let (tx, rx) = make_channel::<Ctx>(Capacity::Unbounded);
        senders.push(tx);
        receivers.push(rx);
        priorities.push(0);
    } else {
        for spec in &builder.channels {
            let (tx, rx) = make_channel::<Ctx>(spec.capacity);
            senders.push(tx);
            receivers.push(rx);
            priorities.push(spec.priority);
        }
    }

    let handler = TaskHandler {
        senders: senders.into(),
    };

    tokio::task::spawn_local(async move {
        while let Some(ctx) = scheduler.next_ctx(&mut receivers, &priorities).await {
            task.handle(ctx).await;
        }
    });

    handler
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Drive the reactor with a gated task so all messages get enqueued
    /// before the scheduler sees any.
    fn run_with_gate<S>(
        channels: Vec<(u8, Capacity, Vec<u32>)>,
        scheduler: S,
    ) -> Vec<u32>
    where
        S: Scheduler<u32>,
    {
        use tokio::sync::Notify;

        let log = Arc::new(Mutex::new(Vec::<u32>::new()));

        struct Gated {
            log: Arc<Mutex<Vec<u32>>>,
            gate: Arc<Notify>,
            gate_passed: bool,
        }

        impl Task<u32> for Gated {
            async fn handle(&mut self, ctx: u32) {
                if !self.gate_passed {
                    self.gate.notified().await;
                    self.gate_passed = true;
                }
                self.log.lock().unwrap().push(ctx);
            }
        }

        let gate = Arc::new(Notify::new());
        let task = Gated {
            log: log.clone(),
            gate: gate.clone(),
            gate_passed: false,
        };

        let reactor = Reactor::<u32, Gated>::new_current().unwrap();

        let mut builder = TaskBuilder::<u32>::new();
        let tokens: Vec<Channel<u32>> = channels
            .iter()
            .map(|(prio, cap, _)| builder.add_channel(*prio, *cap))
            .collect();

        let handler = reactor.spawn_with(task, builder, scheduler).unwrap();

        // Dummy sentinel on first channel to force scheduler into the gate.
        handler.send(tokens[0], u32::MAX).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        for (i, (_prio, _cap, msgs)) in channels.iter().enumerate() {
            for m in msgs {
                handler.send(tokens[i], *m).unwrap();
            }
        }

        gate.notify_one();

        let expected: usize = 1 + channels.iter().map(|(_, _, m)| m.len()).sum::<usize>();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if log.lock().unwrap().len() >= expected {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "timeout: got {} / {} items",
                    log.lock().unwrap().len(),
                    expected
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        drop(handler);
        reactor.shutdown().unwrap();

        let mut recorded = log.lock().unwrap().clone();
        assert_eq!(recorded.remove(0), u32::MAX);
        recorded
    }

    #[test]
    fn fair_priority_order_default() {
        let result = run_with_gate(
            vec![
                (0, Capacity::Unbounded, vec![10, 11, 12]),
                (1, Capacity::Unbounded, vec![20, 21, 22]),
                (2, Capacity::Unbounded, vec![30, 31, 32]),
            ],
            FairPriority::default(),
        );
        assert_eq!(result, vec![10, 11, 12, 20, 21, 22, 30, 31, 32]);
    }

    #[test]
    fn strict_priority_scheduler() {
        let result = run_with_gate(
            vec![
                (0, Capacity::Unbounded, (0..50).collect()),
                (2, Capacity::Unbounded, vec![999]),
            ],
            StrictPriority,
        );

        // StrictPriority: all 50 high-prio items come out before the low one.
        let pos = result.iter().position(|&v| v == 999).unwrap();
        assert_eq!(pos, 50, "low-prio must come last under StrictPriority");
    }

    #[test]
    fn fair_priority_anti_starvation() {
        let high: Vec<u32> = (0..40).map(|i| 1000 + i).collect();
        let low = vec![9999_u32];

        let result = run_with_gate(
            vec![
                (0, Capacity::Unbounded, high.clone()),
                (2, Capacity::Unbounded, low),
            ],
            FairPriority::new(16),
        );

        let pos = result.iter().position(|&v| v == 9999).unwrap();
        assert!(
            pos <= 16,
            "FairPriority must relieve starvation within budget: pos={pos}"
        );

        let highs: Vec<u32> = result.iter().copied().filter(|&v| v != 9999).collect();
        let mut sorted = highs.clone();
        sorted.sort();
        assert_eq!(sorted, high);
    }

    #[test]
    fn bounded_channel_applies_backpressure() {
        // Bounded(1) channel. The consumer blocks on the *first* item only;
        // that leaves the pipeline with:
        //   - 1 item in-flight (held by the handler, awaiting release)
        //   - 1 item buffered (capacity = 1)
        // A further `try_send` must then return `Full`.
        use tokio::sync::Notify;

        struct Slow {
            release: Arc<Notify>,
            blocked_once: bool,
        }
        impl Task<u32> for Slow {
            async fn handle(&mut self, _ctx: u32) {
                if !self.blocked_once {
                    self.blocked_once = true;
                    self.release.notified().await;
                }
            }
        }

        let release = Arc::new(Notify::new());
        let reactor = Reactor::<u32, Slow>::new_current().unwrap();
        let mut builder = TaskBuilder::<u32>::new();
        let ch = builder.add_bounded(0, 1);
        let handler = reactor
            .spawn(
                Slow {
                    release: release.clone(),
                    blocked_once: false,
                },
                builder,
            )
            .unwrap();

        // Consumer picks up item 1 and blocks on `notified`.
        handler.try_send(ch, 1).expect("send 1");
        std::thread::sleep(Duration::from_millis(100));

        // Fill the 1-slot buffer.
        handler.try_send(ch, 2).expect("send 2 fills buffer");
        // Now Full.
        match handler.try_send(ch, 3) {
            Err(TrySendError::Full(3)) => { /* expected */ }
            other => panic!("expected Full(3), got {:?}", other.map(|_| ())),
        }

        // Release the consumer; subsequent items process instantly.
        release.notify_one();
        drop(handler);
        reactor.shutdown().expect("shutdown");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_async_awaits_capacity() {
        // Reactor on its own thread, task that processes each item after a
        // small sleep. A bounded(1) channel; producer uses send_async to
        // push 5 items, which should all land eventually.
        struct Trickle {
            log: Arc<Mutex<Vec<u32>>>,
        }
        impl Task<u32> for Trickle {
            async fn handle(&mut self, ctx: u32) {
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.log.lock().unwrap().push(ctx);
            }
        }

        let log = Arc::new(Mutex::new(Vec::<u32>::new()));
        let reactor = Reactor::<u32, Trickle>::new_current().unwrap();
        let mut builder = TaskBuilder::<u32>::new();
        let ch = builder.add_bounded(0, 1);
        // Use the async spawn variant since we're inside a tokio runtime.
        let handler = reactor
            .spawn_async(Trickle { log: log.clone() }, builder)
            .await
            .unwrap();

        for i in 0..5u32 {
            handler.send_async(ch, i).await.unwrap();
        }

        // Wait for the task to drain.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if log.lock().unwrap().len() == 5 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("didn't drain in time");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(*log.lock().unwrap(), vec![0, 1, 2, 3, 4]);
        drop(handler);
        // Move the blocking shutdown to a blocking thread to avoid
        // stalling the current-thread runtime.
        tokio::task::spawn_blocking(move || reactor.shutdown().unwrap())
            .await
            .unwrap();
    }

    #[test]
    fn shutdown_is_clean() {
        struct Noop;
        impl Task<()> for Noop {
            async fn handle(&mut self, _ctx: ()) {}
        }

        let reactor = Reactor::<(), Noop>::new_current().unwrap();
        let mut builder = TaskBuilder::<()>::new();
        let ch = builder.add_unbounded(0);
        let handler = reactor.spawn(Noop, builder).unwrap();

        for _ in 0..10 {
            handler.send(ch, ()).unwrap();
        }

        drop(handler);
        reactor.shutdown().expect("shutdown clean");
    }
}
