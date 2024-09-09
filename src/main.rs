use std::time::Duration;
use tokio::sync::oneshot;
use reactor::{Task, LocalSpawner};

struct FileReq {
    id: usize,
}

struct FileResp {
    id: usize,
}

struct TaskContext {
    req: FileReq,
    resp: FileResp,
}

struct File {
    id: usize,
}

impl File {
    fn new(id: usize) -> Self {
        Self { id: id, }
    }
}

impl Task<TaskContext> for File {
    async fn handler(&mut self, ctx: TaskContext) {
        let id = self.id;
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::new(1, 0)).await;
            println!("file id {}, req: {}, resp: {}", id, ctx.req.id, ctx.resp.id);
        });
    }
}

fn main() {
	let spawner = LocalSpawner::new_current();
	let (tx, rx) = oneshot::channel();
    let file = File::new(1);
    spawner.spawn(file, tx);
    let handler1 = rx.blocking_recv().expect("failed to get back file handler");

	let (tx, rx) = oneshot::channel();
    let file = File::new(2);
    spawner.spawn(file, tx);
    let handler2 = rx.blocking_recv().expect("failed to get back file handler");

    for x in 0..10 {
        let fh = handler1.clone();
        std::thread::spawn(move || {
            let req = FileReq {
                id: x,
            };
            let resp = FileResp {
                id: x,
            };
            let ctx = TaskContext {
                req: req,
                resp: resp,
            };
            let _ = fh.send(ctx);
        });
    }

    for x in 10..20 {
        let fh = handler2.clone();
        std::thread::spawn(move || {
            let req = FileReq {
                id: x,
            };
            let resp = FileResp {
                id: x,
            };
            let ctx = TaskContext {
                req: req,
                resp: resp,
            };
            let _ = fh.send(ctx);
        });
    }

    std::thread::sleep(Duration::new(2, 0));
    drop(handler1);
    drop(handler2);
    drop(spawner);
    println!("handler dropped");

    std::thread::sleep(Duration::new(10, 0));
}
