use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

// based on example code:
// https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html#use-inside-tokiospawn

/// Handler return to user.
///
/// Use [`TaskHandler::send`] to submit task context into main handler loop.
pub struct TaskHandler<T> {
    tx: mpsc::UnboundedSender<T>,
    highprio_tx: mpsc::UnboundedSender<T>,
}

impl<T> Clone for TaskHandler<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            highprio_tx: self.highprio_tx.clone(),
        }
    }
}

impl<T> TaskHandler<T> {
    pub fn send(&self, ctx: T) {
        let _ = self.tx.send(ctx);
    }

    pub fn send_highprio(&self, ctx: T) {
        let _ = self.highprio_tx.send(ctx);
    }
}

pub trait Task<T: 'static> {

    /// Handler need to be implement by user to process a single task.
    ///
    /// **Be careful:** since this handler running inside of a single thread,
    /// never blocking it, all IO operations should be spawn out.
    fn handler(&mut self, ctx: T) -> impl std::future::Future<Output = ()>;

    /// Start main handler loop.
    fn start(mut self) -> TaskHandler<T> where Self: Sized + 'static {
        let (tx, mut rx) = mpsc::unbounded_channel::<T>();
        let (highprio_tx, mut highprio_rx) = mpsc::unbounded_channel::<T>();
        tokio::task::spawn_local(async move {
            while let Some(ctx) = futures_lite::future::or(highprio_rx.recv(), rx.recv()).await {
                self.handler(ctx).await;
            }
        });
        TaskHandler { tx: tx, highprio_tx: highprio_tx, }
    }
}

pub struct LocalSpawner<C, T> {
   send: mpsc::UnboundedSender<(T, oneshot::Sender<TaskHandler<C>>)>,
}

impl<C, T> Clone for LocalSpawner<C, T> {
    fn clone(&self) -> Self {
        Self {
            send: self.send.clone(),
        }
    }
}

impl<C: 'static + Send, T: Task<C> + 'static + Send> LocalSpawner<C, T> {
    /// Create a new `LocalSpawner` instance use `[tokio::runtime::Builder::new_current_thread]`.
    pub fn new_current() -> Self {
        Self::new(None)
    }

    /// Create a new `LocalSpawner` instance use supplied `[tokio::runtime::Runtime]`.
    pub fn new(runtime: Option<Arc<Runtime>>) -> Self {
        let (send, mut recv) = mpsc::unbounded_channel::<(T, oneshot::Sender<TaskHandler<C>>)>();

        let rt = if let Some(r) = runtime {
            r.clone()
        } else {
            let r = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            Arc::new(r)
        };

        std::thread::spawn(move || {
            let local = LocalSet::new();

            local.spawn_local(async move {
                while let Some((task, tx)) = recv.recv().await {
                    let s = task.start();
                    let _ = tx.send(s);
                }
                // If the while loop returns, then all the LocalSpawner
                // objects have been dropped.
            });

            // This will return once all senders are dropped and all
            // spawned tasks have returned.
            rt.block_on(local);
        });

        Self {
            send,
        }
    }

    /// Spawn a task
    ///
    /// This will kick task to start it's main handler loop.
    ///
    /// User need to prepare a `[tokio::sync::oneshot]` channel to receive notification
    /// when task successful started.
    ///
    /// # Example
    ///
    /// ```
    /// let spawner = LocalSpawner::new_current();
    /// let (tx, rx) = oneshot::channel();
    /// let file = File::new(1);
    /// spawner.spawn(file, tx);
    /// let handler1 = rx.blocking_recv().expect("failed to get back file handler");
    /// ```
    pub fn spawn(&self, task: T, tx: oneshot::Sender<TaskHandler<C>>) {
        self.send.send((task, tx)).expect("Thread with LocalSet has shut down.");
    }
}
