#![cfg(test)]

use std::io::Cursor;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;

use tokio::io::{AsyncRead, AsyncWrite};

use tokio_util::codec::Decoder;

use crate::common::{Map, WrappedRcRefCell};
use crate::scheduler::protocol::{TaskAssignment, TaskId};
use crate::server::core::Core;
use crate::server::dask::dasktransport::{deserialize_packet, Batch, DaskCodec, FromDaskTransport};

use crate::scheduler::ToSchedulerMessage;
use crate::server::protocol::messages::worker::{TaskFinishedMsg, ToWorkerMessage};
use crate::server::reactor::{on_assignments, on_new_tasks, on_new_worker, on_task_finished};
use crate::server::comm::Comm;
use crate::server::task::{ErrorInfo, TaskRef};
use crate::server::worker::{WorkerId, Worker};

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

pub fn task(id: TaskId) -> TaskRef {
    task_with_deps(id, &[])
}

pub fn task_with_deps(id: TaskId, deps: &[TaskId]) -> TaskRef {
    TaskRef::new(
        id,
        Vec::new(),
        deps.to_vec(),
        Default::default(),
        Default::default(),
    )
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

#[derive(Default)]
pub struct TestComm {
    pub scheduler_msgs: Vec<ToSchedulerMessage>,
    pub worker_msgs: Map<WorkerId, Vec<ToWorkerMessage>>,
    pub broadcast_msgs: Vec<ToWorkerMessage>,

    pub client_task_finished: Vec<TaskId>,
    pub client_task_removed: Vec<TaskId>,
    pub client_task_errors: Vec<(TaskId, Vec<TaskId>, ErrorInfo)>,
}

impl TestComm {
    pub fn take_worker_msgs(&mut self, worker_id: WorkerId, len: usize) -> Vec<ToWorkerMessage> {
        let msgs = match self.worker_msgs.remove(&worker_id) {
            None => {
                panic!("No messages for worker {}", worker_id)
            }
            Some(x) => x,
        };
        assert_eq!(msgs.len(), len);
        msgs
    }

    pub fn take_broadcasts(&mut self, len: usize) -> Vec<ToWorkerMessage> {
        assert_eq!(self.broadcast_msgs.len(), len);
        std::mem::take(&mut self.broadcast_msgs)
    }

    pub fn take_scheduler_msgs(&mut self, len: usize) -> Vec<ToSchedulerMessage> {
        assert_eq!(self.scheduler_msgs.len(), len);
        std::mem::take(&mut self.scheduler_msgs)
    }

    pub fn take_scheduler_removal(&mut self) -> Vec<TaskId> {
        let mut result = Vec::new();
        self.scheduler_msgs = std::mem::take(&mut self.scheduler_msgs)
            .into_iter()
            .filter(|m| match m {
                ToSchedulerMessage::RemoveTask(task_id) => {
                    result.push(*task_id);
                    false
                }
                _ => true,
            })
            .collect();
        result
    }

    pub fn take_client_task_finished(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.client_task_finished.len(), len);
        std::mem::take(&mut self.client_task_finished)
    }

    pub fn take_client_task_removed(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.client_task_removed.len(), len);
        std::mem::take(&mut self.client_task_removed)
    }

    pub fn take_client_task_errors(&mut self, len: usize) -> Vec<(TaskId, Vec<TaskId>, ErrorInfo)> {
        assert_eq!(self.client_task_errors.len(), len);
        std::mem::take(&mut self.client_task_errors)
    }

    pub fn emptiness_check(&self) {
        if !self.worker_msgs.is_empty() {
            let ids: Vec<_> = self.worker_msgs.keys().collect();
            panic!("Unexpected worker messages for workers: {:?}", ids);
        }
        assert!(self.scheduler_msgs.is_empty());
        assert!(self.broadcast_msgs.is_empty());

        assert!(self.client_task_finished.is_empty());
        assert!(self.client_task_removed.is_empty());
        assert!(self.client_task_errors.is_empty());
    }
}

impl Comm for TestComm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        let message = rmp_serde::from_slice(&data).unwrap();
        self.worker_msgs.entry(worker_id).or_default().push(message);
    }

    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        let message = rmp_serde::from_slice(&data).unwrap();
        self.broadcast_msgs.push(message);
    }

    fn send_scheduler_message(&mut self, message: ToSchedulerMessage) {
        self.scheduler_msgs.push(message);
    }

    fn send_client_task_finished(&mut self, task_id: TaskId) {
        self.client_task_finished.push(task_id);
    }

    fn send_client_task_removed(&mut self, task_id: TaskId) {
        self.client_task_removed.push(task_id);
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers: Vec<TaskId>,
        error_info: ErrorInfo,
    ) {
        self.client_task_errors
            .push((task_id, consumers, error_info));
    }
}

pub fn create_test_comm() -> TestComm {
    TestComm::default()
}

pub fn create_test_workers(core: &mut Core, cpus: &[u32]) {
    for (i, c) in cpus.iter().enumerate() {
        let worker_id = (100 + i) as WorkerId;
        let worker = Worker::new(worker_id, *c, format!("test{}:123", i));
        on_new_worker(core, &mut TestComm::default(), worker);
    }
}

pub fn submit_test_tasks(core: &mut Core, tasks: &[TaskRef]) {
    on_new_tasks(core, &mut TestComm::default(), tasks.to_vec());
}

pub fn start_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let mut comm = TestComm::default();
    on_assignments(
        core,
        &mut comm,
        vec![TaskAssignment {
            task: task_id,
            worker: worker_id,
            priority: 0,
        }],
    );
}

pub fn finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let mut comm = TestComm::default();
    on_task_finished(
        core,
        &mut comm,
        worker_id,
        TaskFinishedMsg {
            id: task_id,
            size: 1000,
        },
    );
}

pub fn start_and_finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    start_on_worker(core, task_id, worker_id);
    finish_on_worker(core, task_id, worker_id);
}

pub fn submit_example_1(core: &mut Core) {
    /*
       11  12 <- keep
        \  / \
         13  14
         /\  /
        16 15 <- keep
        |
        17
    */

    let t1 = task(11);
    let t2 = task(12);
    t2.get_mut().increment_keep_counter();
    let t3 = task_with_deps(13, &[11, 12]);
    let t4 = task_with_deps(14, &[12]);
    let t5 = task_with_deps(15, &[13, 14]);
    t5.get_mut().increment_keep_counter();
    let t6 = task_with_deps(16, &[13]);
    let t7 = task_with_deps(17, &[16]);
    submit_test_tasks(core, &[t1, t2, t3, t4, t5, t6, t7]);
}

pub fn sorted_vec<T: Ord>(mut vec: Vec<T>) -> Vec<T> {
    vec.sort();
    vec
}
