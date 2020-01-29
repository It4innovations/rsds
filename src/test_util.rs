#![cfg(test)]

use crate::common::WrappedRcRefCell;
use crate::core::CoreRef;
use crate::protocol::protocol::{
    deserialize_packet, serialize_single_packet, Batch, DaskCodec, DaskPacket, FromDaskTransport,
    ToDaskTransport,
};
use crate::scheduler::ToSchedulerMessage;
use bytes::BytesMut;
use std::io::Cursor;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
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

pub fn dummy_core() -> (
    CoreRef,
    tokio::sync::mpsc::UnboundedReceiver<Vec<ToSchedulerMessage>>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (CoreRef::new(tx), rx)
}

pub fn dummy_address() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}

pub fn packets_to_bytes(packets: Vec<DaskPacket>) -> crate::Result<Vec<u8>> {
    let mut data = BytesMut::new();
    let mut codec = DaskCodec::default();
    for packet in packets {
        codec.encode(packet, &mut data)?;
    }
    Ok(data.to_vec())
}
pub fn msg_to_bytes<T: ToDaskTransport>(item: T) -> crate::Result<Vec<u8>> {
    let packet = serialize_single_packet(item)?;
    let mut data = BytesMut::new();
    DaskCodec::default().encode(packet, &mut data)?;
    Ok(data.to_vec())
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
