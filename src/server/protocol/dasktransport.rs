use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::trace::{trace_packet_receive, trace_packet_send};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::buf::BufMutExt;
use futures::sink::WithFlatMap;
use futures::stream::Map;
use std::fs::File;
use std::hash::Hash;
use std::io::Write;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder};
use tokio_util::codec::{FramedRead, FramedWrite};
use crate::common::data::SerializationType;

/// Commonly used types
pub type Frame = BytesMut;
pub type Frames = Vec<Frame>;
pub type Batch<T> = SmallVec<[T; 2]>;

type Endianness = LittleEndian;

/// Low level (de)serialization
#[derive(Debug, Default)]
pub struct DaskPacket {
    pub main_frame: Frame,
    pub additional_frames: Frames,
}

impl DaskPacket {
    pub fn new(main_frame: Frame, additional_frames: Frames) -> Self {
        DaskPacket {
            main_frame,
            additional_frames,
        }
    }

    pub fn from_wrapper<T: Serialize>(
        message: MessageWrapper<T>,
        additional_frames: Frames,
    ) -> crate::Result<Self> {
        let mut main_frame = BytesMut::default().writer();
        rmp_serde::encode::write_named(&mut main_frame, &message)?;
        Ok(DaskPacket {
            main_frame: main_frame.into_inner(),
            additional_frames,
        })
    }
    pub fn from_batch<T: ToDaskTransport>(batch: Batch<T>) -> crate::Result<DaskPacket> {
        let mut builder: MessageBuilder<T::Transport> = MessageBuilder::default();
        for item in batch {
            item.to_transport(&mut builder);
        }
        builder.build_batch()
    }
    pub fn from_simple<T: ToDaskTransport>(item: T) -> crate::Result<DaskPacket> {
        let mut builder: MessageBuilder<T::Transport> = MessageBuilder::default();
        item.to_transport(&mut builder);
        builder.build_single()
    }

    pub fn frame_count(&self) -> usize {
        2 + self.additional_frames.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.header_bytes() + self.payload_bytes()
    }

    pub fn payload_bytes(&self) -> usize {
        self.main_frame.len()
            + self
                .additional_frames
                .iter()
                .map(|f| f.len())
                .sum::<usize>()
    }
    pub fn header_bytes(&self) -> usize {
        (self.frame_count() + 1) * 8 // size for each frame + number of frames
    }
}

const WRITE_BUFFER_SIZE: usize = 32 * 1024;
const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Default)]
struct CodecDecoder {
    frame_index: usize,
    main_message: Frame,
    other_messages: Frames,
}

/// Encoder/decoder
#[derive(Default)]
pub struct DaskCodec {
    sizes: Option<(u64, Vec<u64>)>,
    decoder: CodecDecoder,
}

pub enum DaskPacketPart {
    HeaderPart {
        frame_sizes: Vec<usize>,
        views: Vec<Bytes>,
        max_part_size: usize,
    },
    PayloadPart {
        views: Vec<Bytes>,
        max_part_size: usize,
    },
}

impl Decoder for DaskCodec {
    type Item = DaskPacket;
    type Error = crate::Error;

    fn decode(&mut self, src: &mut BytesMut) -> crate::Result<Option<Self::Item>> {
        let src = if self.sizes.is_none() {
            let size = src.len() as u64;
            if size < 8 {
                return Ok(None);
            }
            let mut cursor = std::io::Cursor::new(src);
            // Following read_u64 cannot failed, hence do not propagate and leave .unwrap() here
            let count: u64 = cursor.read_u64::<Endianness>().unwrap();
            let header_size = (count + 1) * 8;
            if size < header_size {
                return Ok(None);
            }
            let first_size = cursor.read_u64::<Endianness>().unwrap();
            assert_eq!(first_size, 0);
            let main_size = cursor.read_u64::<Endianness>().unwrap();
            let mut total_size: u64 = main_size;
            let mut sizes = Vec::with_capacity((count - 2) as usize);
            for _ in 2..count {
                let size = cursor.read_u64::<Endianness>().unwrap();
                sizes.push(size);
                total_size += size;
            }
            trace_packet_receive(total_size as usize);

            // preallocate space
            self.decoder.main_message = BytesMut::with_capacity(main_size as usize);
            self.decoder.other_messages.clear();
            for size in &sizes {
                self.decoder
                    .other_messages
                    .push(BytesMut::with_capacity(*size as usize));
            }
            self.decoder.frame_index = 0;

            self.sizes = Some((main_size, sizes));
            let src = cursor.into_inner();
            src.advance(header_size as usize);
            src
        } else {
            src
        };
        let max_part_size = READ_BUFFER_SIZE;

        loop {
            let remaining = src.len();
            if remaining == 0 {
                src.reserve(max_part_size);
                return Ok(None);
            }
            let buffer = match self.decoder.frame_index {
                0 => &mut self.decoder.main_message,
                index => &mut self.decoder.other_messages[index - 1],
            };
            let buf_remaining = buffer.capacity() - buffer.len();
            let to_copy = std::cmp::min(remaining, buf_remaining);
            buffer.put_slice(&src.bytes()[..to_copy]);
            src.advance(to_copy);
            if buffer.len() == buffer.capacity() {
                if self.decoder.frame_index == self.decoder.other_messages.len() {
                    self.sizes = None;
                    return Ok(Some(DaskPacket {
                        main_frame: std::mem::take(&mut self.decoder.main_message),
                        additional_frames: std::mem::take(&mut self.decoder.other_messages),
                    }));
                } else {
                    self.decoder.frame_index += 1;
                }
            }
        }
    }
}
impl Encoder for DaskCodec {
    type Item = DaskPacketPart;
    type Error = crate::Error;

    fn encode(&mut self, data: Self::Item, dst: &mut BytesMut) -> crate::Result<()> {
        let views = match data {
            DaskPacketPart::HeaderPart {
                frame_sizes,
                views,
                max_part_size,
            } => {
                let frames = frame_sizes.len();
                let payload_size = frame_sizes.iter().sum::<usize>();
                let byte_count = (frames + 1) * 8 + payload_size;

                trace_packet_send(byte_count);

                dst.reserve(max_part_size);
                dst.put_u64_le(frames as u64);
                for size in frame_sizes {
                    dst.put_u64_le(size as u64);
                }

                views
            }
            DaskPacketPart::PayloadPart {
                views,
                max_part_size,
            } => {
                dst.reserve(max_part_size);
                views
            }
        };

        for view in views {
            dst.extend_from_slice(&view);
        }
        Ok(())
    }
}

/// High-level (de)serialization

/// Wrapper that holds either a single message or a list of messages.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageWrapper<T> {
    MessageList(Batch<T>),
    Message(T),
}

/// Binary data serialized either inline or in a frame.
/// This is the in-flight variant of serialized data.
#[cfg_attr(test, derive(PartialEq))]
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SerializedTransport {
    Indexed {
        #[serde(rename = "_$findex")]
        frame_index: u64,
        #[serde(rename = "_$fcount")]
        frame_count: u64,
        #[serde(rename = "_$header")]
        header: rmpv::Value,
    },
    Inline(rmpv::Value),
}

impl SerializedTransport {
    pub fn to_memory(self, frames: &mut Frames) -> SerializedMemory {
        match self {
            SerializedTransport::Inline(value) => SerializedMemory::Inline(value),
            SerializedTransport::Indexed {
                frame_index,
                frame_count,
                header,
            } => {
                let frames = frames[frame_index as usize..(frame_index + frame_count) as usize]
                    .iter_mut()
                    .map(|frame| std::mem::take(frame));

                SerializedMemory::Indexed {
                    frames: frames.collect(),
                    header,
                }
            }
        }
    }
}

/// Binary data serialized either inline or in a frame.
/// This is the in-memory variant of serialized data.
#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug)]
pub enum SerializedMemory {
    Indexed { frames: Frames, header: rmpv::Value },
    Inline(rmpv::Value),
}

impl SerializedMemory {
    #[inline]
    pub fn to_transport_clone<T: Serialize>(
        &self,
        message_builder: &mut MessageBuilder<T>,
    ) -> SerializedTransport {
        message_builder.copy_serialized(self)
    }
    #[inline]
    pub fn to_transport<T: Serialize>(
        self,
        message_builder: &mut MessageBuilder<T>,
    ) -> SerializedTransport {
        message_builder.take_serialized(self)
    }
}

/// Trait which can convert an associated deserializable type into itself.
pub trait FromDaskTransport {
    type Transport: DeserializeOwned;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self;
}

#[inline]
pub fn map_from_transport<K: Eq + Hash>(
    map: crate::common::Map<K, SerializedTransport>,
    frames: &mut Frames,
) -> crate::common::Map<K, SerializedMemory> {
    map.into_iter()
        .map(|(k, v)| (k, v.to_memory(frames)))
        .collect()
}
#[inline]
pub fn map_to_transport<K: Eq + Hash, T: Serialize>(
    map: crate::common::Map<K, SerializedMemory>,
    message_builder: &mut MessageBuilder<T>,
) -> crate::common::Map<K, SerializedTransport> {
    map.into_iter()
        .map(|(k, v)| (k, v.to_transport(message_builder)))
        .collect()
}
#[inline]
pub fn map_to_transport_clone<K: Eq + Hash + Clone, T: Serialize>(
    map: &crate::common::Map<K, SerializedMemory>,
    message_builder: &mut MessageBuilder<T>,
) -> crate::common::Map<K, SerializedTransport> {
    map.iter()
        .map(|(k, v)| (k.clone(), v.to_transport_clone(message_builder)))
        .collect()
}

/// Message building
/// Trait which can convert itself into an associated serializable type.
pub trait ToDaskTransport {
    type Transport: Serialize;

    fn to_transport(self, message_builder: &mut MessageBuilder<Self::Transport>);
}

impl<T: Serialize> ToDaskTransport for T {
    type Transport = Self;

    fn to_transport(self, message_builder: &mut MessageBuilder<Self::Transport>) {
        message_builder.add_message(self);
    }
}

pub struct MessageBuilder<T> {
    messages: Batch<T>,
    frames: Frames,
}

impl<T> Default for MessageBuilder<T> {
    fn default() -> Self {
        Self {
            messages: Default::default(),
            frames: Default::default(),
        }
    }
}

impl<T: Serialize> MessageBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Batch::<T>::with_capacity(capacity),
            frames: Default::default(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.frames.is_empty()
    }

    #[inline]
    pub fn add_message(&mut self, message: T) {
        self.messages.push(message);
    }
    pub fn copy_serialized(&mut self, serialized: &SerializedMemory) -> SerializedTransport {
        match serialized {
            SerializedMemory::Inline(value) => SerializedTransport::Inline(value.clone()),
            SerializedMemory::Indexed { frames, header } => {
                let frame_index = self.frames.len() as u64;
                let frame_count = frames.len() as u64;
                self.frames.extend_from_slice(&frames);
                SerializedTransport::Indexed {
                    frame_index,
                    frame_count,
                    header: header.clone(),
                }
            }
        }
    }
    pub fn take_serialized(&mut self, serialized: SerializedMemory) -> SerializedTransport {
        match serialized {
            SerializedMemory::Inline(value) => SerializedTransport::Inline(value),
            SerializedMemory::Indexed { mut frames, header } => {
                let frame_index = self.frames.len() as u64;
                let frame_count = frames.len() as u64;
                self.frames.append(&mut frames);
                SerializedTransport::Indexed {
                    frame_index,
                    frame_count,
                    header,
                }
            }
        }
    }

    pub fn build_single(mut self) -> crate::Result<DaskPacket> {
        assert_eq!(self.messages.len(), 1);
        let wrapper = MessageWrapper::Message(self.messages.pop().unwrap());

        DaskPacket::from_wrapper(wrapper, self.frames)
    }
    pub fn build_batch(self) -> crate::Result<DaskPacket> {
        assert!(!self.messages.is_empty());
        let wrapper = MessageWrapper::MessageList(self.messages);

        DaskPacket::from_wrapper(wrapper, self.frames)
    }
}

pub fn split_packet_into_parts(packet: DaskPacket, max_part_size: usize) -> Vec<DaskPacketPart> {
    let size = packet.total_bytes();

    let part_count = ((size as f64) / (max_part_size as f64)).ceil() as usize;
    let mut parts = Vec::with_capacity(part_count);

    let mut frame_sizes = Vec::with_capacity(packet.frame_count());
    frame_sizes.push(0);
    frame_sizes.push(packet.main_frame.len());
    frame_sizes.extend(packet.additional_frames.iter().map(|f| f.len()));

    let header_bytes = packet.header_bytes();
    let DaskPacket {
        main_frame,
        mut additional_frames,
    } = packet;

    let mut current_view = main_frame.freeze();
    let mut frame_index = 0;

    for i in 0..part_count {
        let mut views = vec![];
        let mut limit = if i == 0 { header_bytes } else { 0 };

        while limit < max_part_size {
            if current_view.is_empty() {
                if frame_index >= additional_frames.len() {
                    break;
                }
                current_view = std::mem::take(&mut additional_frames[frame_index]).freeze();
                frame_index += 1;
            }

            let remaining = std::cmp::min(max_part_size - limit, current_view.len());
            views.push(current_view.split_to(remaining));
            limit += remaining;
        }

        if i == 0 {
            parts.push(DaskPacketPart::HeaderPart {
                frame_sizes: std::mem::take(&mut frame_sizes),
                views,
                max_part_size: limit,
            });
        } else {
            parts.push(DaskPacketPart::PayloadPart {
                views,
                max_part_size: limit,
            });
        }
    }

    parts
}

fn parse_packet<T: FromDaskTransport>(
    packet: crate::Result<DaskPacket>,
) -> crate::Result<Batch<T>> {
    let mut packet = packet?;
    let message: MessageWrapper<T::Transport> = match rmp_serde::from_slice(&packet.main_frame) {
        Ok(r) => r,
        Err(e) => {
            // TODO: remove
            File::create("error-packet.bin")
                .unwrap()
                .write_all(&packet.main_frame)
                .unwrap();
            return Err(e.into());
        }
    };
    match message {
        MessageWrapper::Message(p) => {
            Ok(smallvec!(T::deserialize(p, &mut packet.additional_frames)))
        }
        MessageWrapper::MessageList(v) => Ok(v
            .into_iter()
            .map(|p| T::deserialize(p, &mut packet.additional_frames))
            .collect()),
    }
}

pub fn asyncread_to_stream<R: AsyncRead>(stream: R) -> FramedRead<R, DaskCodec> {
    FramedRead::new(stream, DaskCodec::default())
}
pub fn dask_parse_stream<T: FromDaskTransport, R: AsyncRead>(
    stream: FramedRead<R, DaskCodec>,
) -> Map<FramedRead<R, DaskCodec>, impl Fn(crate::Result<DaskPacket>) -> crate::Result<Batch<T>>> {
    stream.map(parse_packet)
}

pub trait IntoInner<T> {
    fn into_inner(self) -> T;
}

impl<Si, Item, U, St, F> IntoInner<Si> for WithFlatMap<Si, Item, U, St, F>
where
    Si: Sink<Item>,
    F: FnMut(U) -> St,
    St: Stream<Item = Result<Item, Si::Error>>,
{
    fn into_inner(self) -> Si {
        WithFlatMap::<Si, Item, U, St, F>::into_inner(self)
    }
}

pub fn asyncwrite_to_sink<W: AsyncWrite>(
    sink: W,
) -> impl Sink<DaskPacket, Error = crate::Error> + IntoInner<FramedWrite<W, DaskCodec>> {
    FramedWrite::new(sink, DaskCodec::default()).with_flat_map(|packet| {
        let parts = split_packet_into_parts(packet, WRITE_BUFFER_SIZE);
        futures::stream::iter(parts.into_iter().map(|p| Ok(p)))
    })
}

pub fn serialize_single_packet<T: ToDaskTransport>(item: T) -> crate::Result<DaskPacket> {
    DaskPacket::from_simple(item)
}
pub fn serialize_batch_packet<T: ToDaskTransport>(batch: Batch<T>) -> crate::Result<DaskPacket> {
    DaskPacket::from_batch(batch)
}

pub fn deserialize_packet<T: FromDaskTransport>(mut packet: DaskPacket) -> crate::Result<Batch<T>> {
    let message: MessageWrapper<T::Transport> = rmp_serde::from_slice(&packet.main_frame)?;

    let commands = match message {
        MessageWrapper::Message(p) => smallvec!(T::deserialize(p, &mut packet.additional_frames)),
        MessageWrapper::MessageList(v) => v
            .into_iter()
            .map(|p| T::deserialize(p, &mut packet.additional_frames))
            .collect(),
    };

    Ok(commands)
}

#[cfg(test)]
mod tests {
    use crate::server::protocol::daskmessages::client::{
        task_spec_to_memory, ClientTaskSpec, DirectTaskSpec, FromClientMessage, KeyInMemoryMsg,
        ToClientMessage, UpdateGraphMsg,
    };
    use crate::server::protocol::dasktransport::{
        asyncwrite_to_sink, serialize_single_packet, split_packet_into_parts, Batch, DaskCodec,
        DaskPacket, DaskPacketPart, Frame, MessageWrapper, SerializedMemory, SerializedTransport,
    };
    use crate::Result;
    use bytes::{Buf, BufMut, BytesMut};
    use futures::SinkExt;
    use maplit::hashmap;

    use crate::common::Map;
    use crate::server::protocol::daskmessages::worker::{
        FromWorkerMessage, RegisterWorkerResponseMsg,
    };
    use crate::server::protocol::dasktransport::IntoInner;
    use crate::server::protocol::key::{to_dask_key, DaskKey};
    use crate::test_util::{bytes_to_msg, load_bin_test_data};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::io::Cursor;
    use tokio_util::codec::{Decoder, Encoder};

    impl Clone for DaskPacket {
        fn clone(&self) -> Self {
            Self {
                main_frame: self.main_frame.clone(),
                additional_frames: self.additional_frames.clone(),
            }
        }
    }

    #[tokio::test]
    async fn parse_message_simple() -> Result<()> {
        let mut buf = BytesMut::default();
        buf.put_u64_le(2);
        buf.put_u64_le(0);
        buf.put_u64_le(1);
        buf.put_u8(137u8);

        let mut codec = DaskCodec::default();
        let packet = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(packet.main_frame.to_vec(), vec!(137u8));
        assert!(packet.additional_frames.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn parse_message_multi_frame() -> Result<()> {
        let mut buf = BytesMut::default();
        let sizes: Vec<usize> = vec![13, 17, 2, 1];

        buf.reserve(8 + 8 * (2 + sizes.len()) + 1 + sizes.iter().sum::<usize>());
        buf.put_u64_le((2 + sizes.len()) as u64);
        buf.put_u64_le(0);
        buf.put_u64_le(1);
        for &size in sizes.iter() {
            buf.put_u64_le(size as u64);
        }
        buf.put_u8(137u8);
        for &size in sizes.iter() {
            buf.put_slice(
                &std::iter::repeat(size as u8)
                    .take(size)
                    .collect::<Vec<u8>>(),
            );
        }

        let mut codec = DaskCodec::default();

        let packet = codec.decode(&mut buf)?.unwrap();
        assert_eq!(packet.main_frame.to_vec(), vec!(137u8));
        assert_eq!(packet.additional_frames.len(), sizes.len());
        for (size, frame) in sizes.into_iter().zip(packet.additional_frames.into_iter()) {
            assert_eq!(frame.len(), size);
            assert_eq!(
                frame.to_vec(),
                std::iter::repeat(size)
                    .take(size)
                    .map(|i| i as u8)
                    .collect::<Vec<u8>>()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_message_simple() -> Result<()> {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let message = create_packet(&bytes, &Default::default());
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        let mut parts = split_packet_into_parts(message, 1024);
        codec.encode(parts.pop().unwrap(), &mut res)?;

        let mut expected = BytesMut::new();
        expected.put_u64_le(2);
        expected.put_u64_le(0);
        expected.put_u64_le(bytes.len() as u64);
        expected.extend_from_slice(&bytes);
        assert_eq!(res, expected.to_vec());

        Ok(())
    }
    #[tokio::test]
    async fn write_message_multi_frame() -> Result<()> {
        let bytes: Vec<u8> = vec![1, 2, 3];
        let frames: Vec<Vec<u8>> = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let message = create_packet(&bytes, &frames);
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        let mut parts = split_packet_into_parts(message, 1024);
        codec.encode(parts.pop().unwrap(), &mut res)?;

        let mut expected = BytesMut::new();
        expected.put_u64_le((2 + frames.len()) as u64);
        expected.put_u64_le(0);
        expected.put_u64_le(bytes.len() as u64);
        for frame in &frames {
            expected.put_u64_le(frame.len() as u64);
        }
        expected.extend_from_slice(&bytes);
        for frame in frames {
            expected.extend_from_slice(&frame);
        }
        assert_eq!(res, expected.to_vec());

        Ok(())
    }
    #[tokio::test]
    async fn write_message_split() -> Result<()> {
        let bytes: Vec<u8> = vec![8; 100];
        let frames: Vec<Vec<u8>> = vec![vec![3; 16], vec![4; 25]];
        let message = create_packet(&bytes, &frames);
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        let parts = split_packet_into_parts(message, 64);
        for part in parts {
            codec.encode(part, &mut res)?;
        }

        let mut expected = BytesMut::new();
        expected.put_u64_le((2 + frames.len()) as u64);
        expected.put_u64_le(0);
        expected.put_u64_le(bytes.len() as u64);
        for frame in &frames {
            expected.put_u64_le(frame.len() as u64);
        }
        expected.extend_from_slice(&bytes);
        for frame in frames {
            expected.extend_from_slice(&frame);
        }
        assert_eq!(res, expected.to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn write_message_large_header() -> Result<()> {
        let bytes: Vec<u8> = vec![8; 2];
        let frames: Vec<Vec<u8>> = vec![];
        let message = create_packet(&bytes, &frames);
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        let parts = split_packet_into_parts(message, 2);
        for part in parts {
            codec.encode(part, &mut res)?;
        }

        let mut expected = BytesMut::new();
        expected.put_u64_le((2 + frames.len()) as u64);
        expected.put_u64_le(0);
        expected.put_u64_le(bytes.len() as u64);
        for frame in &frames {
            expected.put_u64_le(frame.len() as u64);
        }
        expected.extend_from_slice(&bytes);
        for frame in frames {
            expected.extend_from_slice(&frame);
        }
        assert_eq!(res, expected.to_vec());

        Ok(())
    }

    #[test]
    fn split_packet_header_too_large() {
        let packet = create_packet(&vec![1, 2, 3], &Default::default());
        split_packet_into_parts(packet, 1);
    }

    #[test]
    fn split_packet_single_part() {
        // header size == 24 B
        let packet = create_packet(&vec![1, 2, 3], &Default::default());
        let parts = split_packet_into_parts(packet.clone(), 27);
        assert_eq!(parts.len(), 1);
        check_part(&packet, &parts[0], vec![(0, 0, 3)]);
    }

    #[test]
    fn split_packet_header_frame() {
        // header size == 24 B
        let packet = create_packet(&vec![1, 2, 3], &Default::default());
        let parts = split_packet_into_parts(packet.clone(), 24);
        assert_eq!(parts.len(), 2);
        check_part(&packet, &parts[0], vec![]);
        check_part(&packet, &parts[1], vec![(0, 0, 3)]);
    }

    #[test]
    fn split_packet_multiple_views() {
        // header size == 48 B
        let packet = create_packet(&vec![1], &vec![vec![2, 3], vec![1, 3], vec![4; 60]]);
        let parts = split_packet_into_parts(packet.clone(), 50);
        assert_eq!(parts.len(), 3);
        check_part(&packet, &parts[0], vec![(0, 0, 1), (1, 0, 1)]);
        check_part(&packet, &parts[1], vec![(1, 1, 1), (2, 0, 2), (3, 0, 47)]);
        check_part(&packet, &parts[2], vec![(3, 47, 13)]);
    }

    #[tokio::test]
    async fn parse_update_graph_1() -> Result<()> {
        let mut main: Batch<FromClientMessage> =
            bytes_to_msg(&load_bin_test_data("data/pandas-update-graph-1.bin"))?;
        assert_eq!(main.len(), 1);
        match main.pop().unwrap() {
            FromClientMessage::UpdateGraph(mut msg) => {
                assert_eq!(
                    msg.keys,
                    vec!("('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".into())
                );
                assert_eq!(
                    msg.dependencies,
                    hashmap! {
                    to_dask_key("('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)") => vec![to_dask_key("('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)")],
                    to_dask_key("('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)") => vec![]
                    }.into_iter().collect()
                );
                let tasks = parse_tasks(&mut msg);
                match tasks[b"('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".as_ref()] {
                    ClientTaskSpec::Serialized(SerializedMemory::Indexed { .. }) => {}
                    _ => panic!(),
                }
                match tasks
                    [b"('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".as_ref()]
                {
                    ClientTaskSpec::Serialized(SerializedMemory::Indexed { .. }) => {}
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }

        Ok(())
    }

    #[tokio::test]
    async fn parse_update_graph_2() -> Result<()> {
        let mut main: Batch<FromClientMessage> =
            bytes_to_msg(&load_bin_test_data("data/pandas-update-graph-2.bin"))?;
        assert_eq!(main.len(), 2);
        main.pop().unwrap();
        match main.pop().unwrap() {
            FromClientMessage::UpdateGraph(mut msg) => {
                assert_eq!(
                    msg.keys,
                    vec!(to_dask_key(
                        "('truediv-fb32c371476f0df11c512c4c98d6380d', 0)"
                    ))
                );
                let tasks = parse_tasks(&mut msg);
                match &tasks[b"('truediv-fb32c371476f0df11c512c4c98d6380d', 0)".as_ref()] {
                    ClientTaskSpec::Direct(DirectTaskSpec {
                        function,
                        args,
                        kwargs,
                    }) => {
                        assert_eq!(
                            hash(&get_binary(function.as_ref().unwrap())),
                            14885086766577267268
                        );
                        assert_eq!(
                            hash(&get_binary(args.as_ref().unwrap())),
                            518960099204433046
                        );
                        assert!(kwargs.is_none());
                    }
                    _ => panic!(),
                }
                match tasks[b"('series-groupby-sum-chunk-series-groupby-sum-agg-345ee905ca52a3462956b295ddd70113', 0)".as_ref()] {
                    ClientTaskSpec::Serialized(SerializedMemory::Indexed { .. }) => {}
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }

        Ok(())
    }

    #[tokio::test]
    async fn parse_heartbeat_int() -> Result<()> {
        let main = load_bin_test_data("data/register-heartbeat-int.bin");
        let msg: MessageWrapper<RegisterWorkerResponseMsg> =
            rmp_serde::from_slice(main.as_slice())?;
        match msg {
            MessageWrapper::Message(v) => {
                assert_eq!(v.heartbeat_interval, 1.0.into());
            }
            _ => panic!(),
        }

        Ok(())
    }

    #[tokio::test]
    async fn parse_steal_response_state_none() -> Result<()> {
        let main = load_bin_test_data("data/steal-response-state-none.bin");
        let msg: MessageWrapper<FromWorkerMessage<SerializedTransport>> =
            rmp_serde::from_slice(main.as_slice())?;
        match msg {
            MessageWrapper::MessageList(v) => {
                assert_eq!(v.len(), 1);
                match &v[0] {
                    FromWorkerMessage::StealResponse(msg) => {
                        assert!(msg.state.is_none());
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }

        Ok(())
    }

    #[tokio::test]
    async fn serialize_key_in_memory() -> Result<()> {
        let msg = ToClientMessage::KeyInMemory(KeyInMemoryMsg {
            key: "hello".into(),
            r#type: vec![1, 2, 3],
        });

        let vec = vec![];
        let sink: Cursor<Vec<u8>> = Cursor::new(vec);
        let mut sink = asyncwrite_to_sink(sink);
        sink.send(serialize_single_packet(msg)?).await?;

        assert_eq!(
            sink.into_inner().into_inner().into_inner(),
            vec![
                2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 0, 0, 131, 162,
                111, 112, 173, 107, 101, 121, 45, 105, 110, 45, 109, 101, 109, 111, 114, 121, 163,
                107, 101, 121, 165, 104, 101, 108, 108, 111, 164, 116, 121, 112, 101, 196, 3, 1, 2,
                3
            ]
        );

        Ok(())
    }

    fn parse_tasks(msg: &mut UpdateGraphMsg) -> Map<DaskKey, ClientTaskSpec<SerializedMemory>> {
        std::mem::take(&mut msg.tasks)
            .into_iter()
            .map(|(k, v)| (k, task_spec_to_memory(v, &mut msg.frames)))
            .collect()
    }

    fn get_binary(serialized: &SerializedMemory) -> Vec<u8> {
        match serialized {
            SerializedMemory::Inline(v) => match v {
                rmpv::Value::Binary(v) => v.clone(),
                _ => panic!("Wrong MessagePack value"),
            },
            _ => panic!("Wrong SerializedMemory type"),
        }
    }

    fn check_part(
        packet: &DaskPacket,
        part: &DaskPacketPart,
        expected: Vec<(usize, usize, usize)>,
    ) {
        let views = match part {
            DaskPacketPart::HeaderPart { views, .. } => views,
            DaskPacketPart::PayloadPart { views, .. } => views,
        };

        assert_eq!(expected.len(), views.len());
        for (view, expected) in views.iter().zip(expected) {
            let (frame, start, len) = expected;
            let mut orig_view = match frame {
                0 => &packet.main_frame,
                index => &packet.additional_frames[index - 1],
            }
            .clone();
            orig_view.advance(start);
            orig_view.truncate(len);
            assert_eq!(*view, orig_view);
        }
    }

    fn create_packet(main_frame: &Vec<u8>, additional_frames: &Vec<Vec<u8>>) -> DaskPacket {
        DaskPacket::new(
            Frame::from(main_frame.as_slice()),
            additional_frames
                .iter()
                .map(|f| Frame::from(f.as_slice()))
                .collect(),
        )
    }

    fn hash(data: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write(data);
        hasher.finish()
    }
}

pub fn make_dask_payload(serializer: SerializationType, data: BytesMut) -> SerializedMemory {
    match serializer {
        SerializationType::None => {
            todo!()
            //TODO: Do not use inline but similar thing as pickle
            //SerializedMemory::Inline(rmpv::Value::Binary(data))
        },
        SerializationType::Pickle => make_dask_pickle_payload(data),
    }
}

pub fn make_dask_pickle_payload(data: BytesMut) -> SerializedMemory {
    let size = data.len();
    SerializedMemory::Indexed {
        frames: vec![data],
        header: rmpv::Value::Map(vec![
            ("serializer".into(), "pickle".into()),
            ("count".into(), 1.into()),
            ("lengths".into(), vec![rmpv::Value::Integer(size.into())].into()),
            ("deserialize".into(), false.into()),
        ])
    }
}