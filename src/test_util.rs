#![cfg(test)]

use std::io::Cursor;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::Decoder;

use crate::common::{Set, WrappedRcRefCell};
use crate::scheduler::protocol::{TaskAssignment, TaskId};
use crate::server::core::{Core, CoreRef};
use crate::server::dask::dasktransport::{deserialize_packet, Batch, DaskCodec, FromDaskTransport};
use crate::server::gateway::Gateway;
use crate::server::notifications::{ClientNotifications, Notifications};
use crate::server::task::TaskRef;
use crate::server::worker::WorkerRef;

/// Memory stream for reading and writing at the same time.
pub struct MemoryStream {
    input: Cursor<Vec<u8>>,
    pub output: WrappedRcRefCell<Vec<u8>>,
}

/*impl MemoryStream {
    pub fn new(input: Vec<u8>) -> (Self, WrappedRcRefCell<Vec<u8>>) {
        let output = WrappedRcRefCell::wrap(Default::default());
        (
            Self {
                input: Cursor::new(input),
                output: output.clone(),
            },
            output,
        )
    }
}*/

impl AsyncRead for MemoryStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.input).poll_read(cx, buf)
    }
}
impl AsyncWrite for MemoryStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_shutdown(cx)
    }
}

pub(crate) fn create_worker(
    core: &mut Core,
    sender: UnboundedSender<Bytes>,
    address: String,
    ncpus: u32,
) -> WorkerRef {
    let worker_ref = WorkerRef::new(core.new_worker_id(), ncpus, sender, address);
    core.register_worker(worker_ref.clone());
    worker_ref
}

pub fn task(id: TaskId) -> TaskRef {
    task_deps(id, &[])
}
pub fn task_deps(id: TaskId, deps: &[&TaskRef]) -> TaskRef {
    let task = TaskRef::new(
        id,
        Vec::new(),
        deps.iter().map(|t| t.get().id).collect(),
        deps.iter().filter(|t| !t.get().is_finished()).count() as u32,
        Default::default(),
        Default::default(),
    );
    for &dep in deps {
        dep.get_mut().add_consumer(task.clone());
    }
    task
}

pub fn worker(core: &mut Core, address: &str) -> (WorkerRef, impl Stream<Item = Bytes>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let worker = create_worker(core, tx, address.to_string(), 1);
    (worker, rx)
}

pub(crate) fn task_add(core: &mut Core, id: TaskId) -> TaskRef {
    task_add_deps(core, id, &[])
}
pub(crate) fn task_add_deps(core: &mut Core, id: TaskId, deps: &[&TaskRef]) -> TaskRef {
    let t = task_deps(id, deps);
    core.add_task(t.clone());
    t
}

pub(crate) fn task_assign(core: &mut Core, task: &TaskRef, worker: &WorkerRef) -> Notifications {
    let mut notifications = Notifications::default();
    let tid = task.get().id;
    let wid = worker.get().id;
    core.process_assignments(
        vec![TaskAssignment {
            task: tid,
            worker: wid,
            priority: 0,
        }],
        &mut notifications,
    );
    notifications
}

pub fn bytes_to_msg<T: FromDaskTransport>(data: &[u8]) -> crate::Result<Batch<T>> {
    let mut bytes = BytesMut::from(data);
    let packet = DaskCodec::default().decode(&mut bytes)?.unwrap();
    deserialize_packet(packet)
}

pub fn load_bin_test_data(path: &str) -> Vec<u8> {
    let path = get_test_path(path);
    std::fs::read(path).unwrap()
}
pub fn get_test_path(path: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(path)
        .to_str()
        .unwrap()
        .to_owned()
}

pub struct TestClientState {
    keep: Set<TaskId>,
}

pub type TestClientStateRef = WrappedRcRefCell<TestClientState>;

pub struct TestGateway {
    state_ref: TestClientStateRef,
}

impl TestClientStateRef {
    pub fn new() -> Self {
        WrappedRcRefCell::wrap(TestClientState {
            keep: Default::default(),
        })
    }

    pub fn get_gateway(&self) -> Box<dyn Gateway> {
        Box::new(TestGateway {
            state_ref: self.clone(),
        })
    }

    pub fn get_core(&self) -> CoreRef {
        CoreRef::new(self.get_gateway())
    }
}

impl Gateway for TestGateway {
    fn is_kept(&self, task_id: TaskId) -> bool {
        self.state_ref.get().keep.contains(&task_id)
    }

    fn send_notifications(&self, _notifications: ClientNotifications) {
        unimplemented!()
    }
}
