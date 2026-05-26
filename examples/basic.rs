//! Basic usage of hyperfile-reactor.
//!
//! Run with: `cargo run --example basic`
//!
//! Demonstrates:
//! - registering multiple channels with different priorities
//! - mixing bounded (backpressured) and unbounded channels
//! - choosing between the default `FairPriority` scheduler and the
//!   explicit `StrictPriority` via `spawn_with`
//! - the async producer API `send_async` (backpressure)

use std::time::Duration;

use hyperfile_reactor::{FairPriority, Reactor, Task, TaskBuilder};

#[derive(Debug)]
struct FileReq {
    id: usize,
}

#[derive(Debug)]
struct FileResp {
    id: usize,
}

#[derive(Debug)]
struct FileCtx {
    req: FileReq,
    resp: FileResp,
}

struct FileTask {
    id: usize,
}

impl FileTask {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

impl Task<FileCtx> for FileTask {
    async fn handle(&mut self, ctx: FileCtx) {
        let id = self.id;
        // Offload the actual work so the single-threaded handler loop is
        // never blocked.
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!(
                "file id {}, req: {}, resp: {}",
                id, ctx.req.id, ctx.resp.id
            );
        });
    }
}

fn main() {
    let reactor = Reactor::<FileCtx, FileTask>::new_current().expect("build reactor");

    // Task 1: uses the default scheduler (FairPriority, budget=16). A
    // bounded "callback" channel gives backpressure at priority 0; high
    // and low are unbounded.
    let mut b1 = TaskBuilder::<FileCtx>::new();
    let cb1 = b1.add_bounded(0, 8);
    let hi1 = b1.add_unbounded(1);
    let lo1 = b1.add_unbounded(2);
    let handler1 = reactor.spawn(FileTask::new(1), b1).expect("spawn 1");

    // Task 2: same channel layout but uses an explicit FairPriority with a
    // smaller budget so starvation relief kicks in sooner. `spawn_with` is
    // how you pick any `Scheduler` — including user-defined ones.
    let mut b2 = TaskBuilder::<FileCtx>::new();
    let _cb2 = b2.add_bounded(0, 4);
    let _hi2 = b2.add_unbounded(1);
    let lo2 = b2.add_unbounded(2);
    let handler2 = reactor
        .spawn_with(FileTask::new(2), b2, FairPriority::new(4))
        .expect("spawn 2");

    // Producer threads for task 1 — mix channels.
    let mut producers = Vec::new();
    for x in 0..10 {
        let fh = handler1.clone();
        let ch = if x % 3 == 0 {
            cb1
        } else if x % 3 == 1 {
            hi1
        } else {
            lo1
        };
        producers.push(std::thread::spawn(move || {
            let ctx = FileCtx {
                req: FileReq { id: x },
                resp: FileResp { id: x },
            };
            // try_send: cb1 is bounded; if full this would return
            // TrySendError::Full. Capacity is generous here so we expect
            // every send to succeed.
            fh.send(ch, ctx).expect("send");
        }));
    }

    // Producer threads for task 2 — only low-prio unbounded, simple.
    for x in 10..20 {
        let fh = handler2.clone();
        producers.push(std::thread::spawn(move || {
            let ctx = FileCtx {
                req: FileReq { id: x },
                resp: FileResp { id: x },
            };
            fh.send(lo2, ctx).expect("send");
        }));
    }

    for p in producers {
        p.join().unwrap();
    }

    // Let the inner spawned tasks finish printing.
    std::thread::sleep(Duration::from_millis(500));

    // Demonstrate backpressure via send_async on task 1's bounded channel.
    // We build a small current-thread runtime just to drive the futures on
    // this thread (the reactor itself lives on its own thread).
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let fh = handler1.clone();
        rt.block_on(async move {
            for x in 100..108u32 {
                let ctx = FileCtx {
                    req: FileReq { id: x as usize },
                    resp: FileResp { id: x as usize },
                };
                fh.send_async(cb1, ctx).await.expect("backpressured send");
            }
        });
    }

    std::thread::sleep(Duration::from_millis(1000));

    drop(handler1);
    drop(handler2);

    reactor.shutdown().expect("shutdown");
    println!("reactor shut down cleanly");
}
