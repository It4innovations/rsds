#![cfg(test)]

use crate::common::WrappedRcRefCell;
use crate::scheduler::protocol::{TaskAssignment, TaskId};
use crate::scheduler::ToSchedulerMessage;
use crate::server::client::{Client, ClientId};
use crate::server::core::{Core, CoreRef};
use crate::server::protocol::daskmessages::client::ClientTaskSpec;
use crate::server::protocol::dasktransport::{
    deserialize_packet, serialize_single_packet, split_packet_into_parts, Batch, DaskCodec,
    DaskPacket, Frame, FromDaskTransport, SerializedMemory, ToDaskTransport,
};
use crate::server::protocol::key::to_dask_key;
use crate::server::task::TaskRef;
use crate::server::worker::{create_worker, WorkerRef};
use crate::server::{comm::CommRef, notifications::Notifications};
use bytes::{BytesMut, Bytes};
use futures::{Stream, StreamExt};
use std::io::Cursor;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::codec::{Decoder, Encoder};

/// Memory stream for reading and writing at the same time.
pub struct MemoryStream {
    input: Cursor<Vec<u8>>,
    pub output: WrappedRcRefCell<Vec<u8>>,
}

impl MemoryStream {
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
}

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

pub fn dummy_ctx() -> (
    CoreRef,
    CommRef,
    tokio::sync::mpsc::UnboundedReceiver<Vec<ToSchedulerMessage>>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (CoreRef::default(), CommRef::new(tx), rx)
}
pub fn dummy_address() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}
/*pub fn dummy_serialized() -> SerializedMemory {
    SerializedMemory::Inline(rmpv::Value::Nil)
}*/

pub fn task(id: TaskId) -> TaskRef {
    task_deps(id, &[])
}
pub fn task_deps(id: TaskId, deps: &[&TaskRef]) -> TaskRef {
    let task = TaskRef::new(
        id,
        format!("{}", id).into(),
        Some(ClientTaskSpec::Serialized(SerializedMemory::Inline(
            rmpv::Value::Nil,
        ))),
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
    let worker = create_worker(core, tx, to_dask_key(address), 1);
    (
        worker,
        rx,
    )
}

pub fn client(id: ClientId) -> (Client, UnboundedReceiver<DaskPacket>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Client::new(id, format!("client-{}", id).into(), tx), rx)
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

pub fn packets_to_bytes(packets: Vec<DaskPacket>) -> crate::Result<Vec<u8>> {
    let mut data = BytesMut::new();
    let mut codec = DaskCodec::default();
    for packet in packets {
        let parts = split_packet_into_parts(packet, 1024);
        for part in parts {
            codec.encode(part, &mut data)?;
        }
    }
    Ok(data.to_vec())
}
pub fn msg_to_bytes<T: ToDaskTransport>(item: T) -> crate::Result<Vec<u8>> {
    let packet = serialize_single_packet(item)?;
    let mut data = BytesMut::new();

    let parts = split_packet_into_parts(packet, 1024);
    let mut codec = DaskCodec::default();
    for part in parts {
        codec.encode(part, &mut data)?;
    }

    Ok(data.to_vec())
}
pub fn bytes_to_msg<T: FromDaskTransport>(data: &[u8]) -> crate::Result<Batch<T>> {
    let mut bytes = BytesMut::from(data);
    let packet = DaskCodec::default().decode(&mut bytes)?.unwrap();
    deserialize_packet(packet)
}
pub fn packet_to_msg<T: FromDaskTransport>(packet: DaskPacket) -> crate::Result<Batch<T>> {
    deserialize_packet(packet)
}
pub fn frame(data: &[u8]) -> Frame {
    BytesMut::from(data)
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
