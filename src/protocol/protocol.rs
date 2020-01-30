use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::util::{OptionExt, ResultExt};
use byteorder::{LittleEndian, ReadBytesExt};
use futures::stream::Map;
use std::fs::File;
use std::hash::Hash;
use std::io::Write;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder};
use tokio_util::codec::{FramedRead, FramedWrite};

/// Commonly used types
pub type Frame = Bytes;
pub type Frames = Vec<Frame>;
pub type Batch<T> = SmallVec<[T; 2]>;

type Endianness = LittleEndian;

/// Low level (de)serialization
#[derive(Debug, Default)]
pub struct DaskPacket {
    pub main_frame: Bytes,
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
        Ok(DaskPacket {
            main_frame: rmp_serde::to_vec_named(&message)?.into(),
            additional_frames,
        })
    }

    pub fn from_batch<T: ToDaskTransport>(batch: Batch<T>) -> crate::Result<DaskPacket> {
        let mut builder: MessageBuilder<T::Transport> = MessageBuilder::new();
        for item in batch {
            item.to_transport(&mut builder);
        }
        builder.build_batch()
    }

    pub fn from_simple<T: ToDaskTransport>(item: T) -> crate::Result<DaskPacket> {
        let mut builder: MessageBuilder<T::Transport> = MessageBuilder::new();
        item.to_transport(&mut builder);
        builder.build_single()
    }
}

/// Encoder/decoder
#[derive(Default)]
pub struct DaskCodec {
    sizes: Option<(u64, Vec<u64>)>,
    main_message: Option<Bytes>,
    other_messages: Frames,
}

impl Decoder for DaskCodec {
    type Item = DaskPacket;
    type Error = crate::DsError;

    fn decode(&mut self, src: &mut BytesMut) -> crate::Result<Option<DaskPacket>> {
        let src = if self.sizes.is_none() {
            let size = src.len() as u64;
            if size < 8 {
                return Ok(None);
            }
            let mut cursor = std::io::Cursor::new(src);
            // Following read_u64 cannot failed, hence do not propagate and leave .unwrap() here
            let count: u64 = cursor.read_u64::<Endianness>().ensure();
            let header_size = (count + 1) * 8;
            if size < header_size {
                return Ok(None);
            }
            let first_size = cursor.read_u64::<Endianness>().ensure();
            assert_eq!(first_size, 0);
            let main_size = cursor.read_u64::<Endianness>().ensure();
            let mut sizes = Vec::new();
            for _ in 2..count {
                sizes.push(cursor.read_u64::<Endianness>().ensure());
            }
            self.sizes = Some((main_size, sizes));
            let src = cursor.into_inner();
            src.advance(header_size as usize);
            src
        } else {
            src
        };

        let (main_size, sizes) = self.sizes.as_ref().ensure();
        if self.main_message.is_none() {
            let size = src.len() as u64;
            if *main_size > size {
                return Ok(None);
            }
            self.main_message = Some(src.split_to(*main_size as usize).freeze());
        }

        for &frame_size in &sizes[self.other_messages.len()..] {
            let size = src.len() as u64;
            if frame_size > size {
                return Ok(None);
            }
            self.other_messages
                .push(src.split_to(frame_size as usize).freeze());
        }
        self.sizes = None;
        Ok(Some(DaskPacket {
            main_frame: self.main_message.take().ensure(),
            additional_frames: std::mem::take(&mut self.other_messages),
        }))
    }
}
impl Encoder for DaskCodec {
    type Item = DaskPacket;
    type Error = crate::DsError;

    fn encode(&mut self, data: DaskPacket, dst: &mut BytesMut) -> crate::Result<()> {
        let frames = 2 + data.additional_frames.len();

        let n = 8 * (frames + 1)
            + data.main_frame.len()
            + data
                .additional_frames
                .iter()
                .map(|i| i.len())
                .sum::<usize>();
        dst.reserve(n);
        dst.put_u64_le(frames as u64);
        dst.put_u64_le(0);
        dst.put_u64_le(data.main_frame.len() as u64);
        for frame in &data.additional_frames {
            dst.put_u64_le(frame.len() as u64);
        }
        dst.extend_from_slice(&data.main_frame);
        for frame in &data.additional_frames {
            dst.extend_from_slice(&frame);
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
                let frames = (0..frame_count).map(|i| {
                    let frame = &frames[(frame_index + i) as usize]; // TODO: avoid copy
                    frame.clone()
                });

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
#[derive(Debug)]
pub enum SerializedMemory {
    Indexed { frames: Frames, header: rmpv::Value },
    Inline(rmpv::Value),
}

impl SerializedMemory {
    pub fn to_transport<T: Serialize>(
        &self,
        message_builder: &mut MessageBuilder<T>,
    ) -> SerializedTransport {
        message_builder.add_serialized(self)
    }
}

/// Trait which can convert an associated deserializable type into itself.
pub trait FromDaskTransport {
    type Transport: DeserializeOwned;

    fn to_memory(source: Self::Transport, frames: &mut Frames) -> Self;
}

impl<T: DeserializeOwned> FromDaskTransport for T {
    type Transport = Self;

    fn to_memory(source: Self::Transport, _frames: &mut Frames) -> Self {
        source
    }
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
pub fn map_ref_to_transport<K: Eq + Hash + Clone, T: Serialize>(
    map: &crate::common::Map<K, SerializedMemory>,
    message_builder: &mut MessageBuilder<T>,
) -> crate::common::Map<K, SerializedTransport> {
    map.iter()
        .map(|(k, v)| (k.clone(), v.to_transport(message_builder)))
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

impl<T: Serialize> MessageBuilder<T> {
    pub fn new() -> Self {
        Self {
            messages: Default::default(),
            frames: Default::default(),
        }
    }

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
    pub fn add_serialized(&mut self, serialized: &SerializedMemory) -> SerializedTransport {
        match serialized {
            SerializedMemory::Inline(value) => SerializedTransport::Inline(value.clone()),
            SerializedMemory::Indexed { frames, header } => {
                let frame_index = self.frames.len() as u64;
                self.frames.extend_from_slice(&frames);
                SerializedTransport::Indexed {
                    frame_index,
                    frame_count: frames.len() as u64,
                    header: header.clone(),
                }
            }
        }
    }

    pub fn build_single(mut self) -> crate::Result<DaskPacket> {
        assert!(!self.messages.is_empty());
        assert_eq!(self.messages.len(), 1);
        let wrapper = MessageWrapper::Message(self.messages.pop().ensure());

        DaskPacket::from_wrapper(wrapper, self.frames)
    }

    pub fn build_batch(self) -> crate::Result<DaskPacket> {
        assert!(!self.messages.is_empty());
        let wrapper = MessageWrapper::MessageList(self.messages);

        DaskPacket::from_wrapper(wrapper, self.frames)
    }
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
        MessageWrapper::Message(p) => Ok(smallvec!(T::to_memory(p, &mut packet.additional_frames))),
        MessageWrapper::MessageList(v) => Ok(v
            .into_iter()
            .map(|p| T::to_memory(p, &mut packet.additional_frames))
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

pub fn asyncwrite_to_sink<W: AsyncWrite>(sink: W) -> FramedWrite<W, DaskCodec> {
    FramedWrite::new(sink, Default::default())
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
        MessageWrapper::Message(p) => smallvec!(T::to_memory(p, &mut packet.additional_frames)),
        MessageWrapper::MessageList(v) => v
            .into_iter()
            .map(|p| T::to_memory(p, &mut packet.additional_frames))
            .collect(),
    };

    Ok(commands)
}

#[cfg(test)]
mod tests {
    use crate::protocol::clientmsg::{
        ClientTaskSpec, FromClientMessage, KeyInMemoryMsg, ToClientMessage,
    };
    use crate::protocol::protocol::{
        asyncread_to_stream, dask_parse_stream, serialize_single_packet, Batch, DaskCodec,
        DaskPacket, FromDaskTransport, SerializedMemory,
    };
    use crate::Result;
    use bytes::{BufMut, BytesMut};
    use futures::{SinkExt, StreamExt};
    use maplit::hashmap;

    use crate::test_util::load_bin_test_data;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::io::Cursor;
    use tokio_util::codec::{Decoder, Encoder, Framed};

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
        let message = DaskPacket::new(bytes::Bytes::from(bytes.clone()), Default::default());
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        codec.encode(message, &mut res)?;

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
        let message = DaskPacket::new(
            bytes::Bytes::from(bytes.clone()),
            frames
                .iter()
                .map(|f| bytes::Bytes::from(f.clone()))
                .collect(),
        );
        let mut res = BytesMut::new();

        let mut codec = DaskCodec::default();
        codec.encode(message, &mut res)?;

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
    async fn parse_update_graph_1() -> Result<()> {
        let main: Batch<FromClientMessage> =
            parse_bytes(load_bin_test_data("data/pandas-update-graph-1.bin")).await;
        assert_eq!(main.len(), 1);
        match &main[0] {
            FromClientMessage::UpdateGraph(msg) => {
                assert_eq!(
                    msg.keys,
                    vec!("('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)")
                );
                assert_eq!(
                    msg.dependencies,
                    hashmap! {
                    "('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".to_owned() => vec!["('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".to_owned()],
                    "('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)".to_owned() => vec![]
                    }.into_iter().collect()
                );
                match &msg.tasks["('len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)"] {
                    ClientTaskSpec::Serialized(SerializedMemory::Indexed { .. }) => {}
                    _ => panic!(),
                }
                match &msg.tasks
                    ["('getitem-len-chunk-make-timeseries-len-agg-14596c0437d9f1e7163f5c12fe93bee8', 0)"]
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
        let main: Batch<FromClientMessage> =
            parse_bytes(load_bin_test_data("data/pandas-update-graph-2.bin")).await;
        assert_eq!(main.len(), 2);
        match &main[0] {
            FromClientMessage::UpdateGraph(msg) => {
                assert_eq!(
                    msg.keys,
                    vec!("('truediv-fb32c371476f0df11c512c4c98d6380d', 0)")
                );
                match &msg.tasks["('truediv-fb32c371476f0df11c512c4c98d6380d', 0)"] {
                    ClientTaskSpec::Direct {
                        function,
                        args,
                        kwargs,
                    } => {
                        assert_eq!(hash(&get_binary(function)), 14885086766577267268);
                        assert_eq!(hash(&get_binary(args)), 518960099204433046);
                        assert!(kwargs.is_none());
                    }
                    _ => panic!(),
                }
                match &msg.tasks["('series-groupby-sum-chunk-series-groupby-sum-agg-345ee905ca52a3462956b295ddd70113', 0)"] {
                    ClientTaskSpec::Serialized(SerializedMemory::Indexed { .. }) => {}
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }

        Ok(())
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

    async fn parse_bytes<T: FromDaskTransport>(data: Vec<u8>) -> Batch<T> {
        dask_parse_stream(asyncread_to_stream(data.as_slice()))
            .next()
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn serialize_key_in_memory() -> Result<()> {
        let msg = ToClientMessage::KeyInMemory(KeyInMemoryMsg {
            key: "hello".to_owned(),
            r#type: vec![1, 2, 3],
        });

        let vec = vec![];
        let sink: Cursor<Vec<u8>> = Cursor::new(vec);
        let mut sink = Framed::new(sink, DaskCodec::default());
        sink.send(serialize_single_packet(msg)?).await?;

        assert_eq!(
            sink.into_inner().into_inner(),
            vec![
                2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 0, 0, 131, 162,
                111, 112, 173, 107, 101, 121, 45, 105, 110, 45, 109, 101, 109, 111, 114, 121, 163,
                107, 101, 121, 165, 104, 101, 108, 108, 111, 164, 116, 121, 112, 101, 196, 3, 1, 2,
                3
            ]
        );

        Ok(())
    }

    fn hash(data: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write(data);
        hasher.finish()
    }
}
