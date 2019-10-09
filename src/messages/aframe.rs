use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use rmp_serde as rmps;
use crate::daskcodec::DaskMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum AfKeyElement {
    Index(u64),
    Attribute(String),
}

impl AfKeyElement {
    pub fn as_index(&self) -> Option<u64> {
        match self {
            Self::Index(index) => Some(*index),
            _ => None
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Attribute(attr) => Some(&attr),
            _ => None
        }
    }
}

type AfKey = Vec<AfKeyElement>;

/*#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum AfKey {
    Single(AfKeyElement),
    Composed(Vec<AfKeyElement>),
}*/

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AfHeader {
    serializer: String,
    compression: Vec<Option<String>>,
    lengths: Vec<u64>,
    count: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AfDescriptor {
    #[serde(with = "tuple_vec_map")]
    pub headers: Vec<(AfKey, AfHeader)>,
    pub keys: Vec<AfKey>,
    pub bytestrings: Vec<AfKey>, // Expected type?
}

impl AfDescriptor {
    // TODO: Migrate to parse_aframes
    pub fn split_frames_by_index(self, additional_frames: Vec<Bytes>) -> crate::Result<HashMap<u64, Vec<AdditionalFrame>>> {
        let mut result: HashMap<u64, Vec<AdditionalFrame>> = HashMap::new();
        for ((key, header), data) in self.headers.into_iter().zip(additional_frames) {
            if key.is_empty() {
                panic!("Key is empty"); // TODO: bail
            }
            let index = key[0].as_index().unwrap_or_else(|| {
                panic!("First element of key is not index"); // TODO bail
            });
            result.entry(index).or_default().push(AdditionalFrame {
                key: key,
                header: header,
                data: data.to_vec(),
            });
        }
        Ok(result)
    }
}

pub fn parse_aframes(additional_frames: Vec<Bytes>) -> HashMap<u64, Vec<AdditionalFrame>> {
    if additional_frames.is_empty() {
        return Default::default();
    }
    let mut iter = additional_frames.into_iter();
    let descriptor: AfDescriptor = rmps::from_slice(&iter.next().unwrap()).unwrap();
    let mut result: HashMap<u64, Vec<AdditionalFrame>> = HashMap::new();

    for ((key, header), data) in descriptor.headers.into_iter().zip(iter) {
        if key.is_empty() {
            panic!("Key is empty"); // TODO: bail (or skip?)
        }
        let index = key[0].as_index().unwrap_or_else(|| {
            panic!("First element of key is not index"); // TODO bail
        });
        result.entry(index).or_default().push(AdditionalFrame {
            key: key,
            header: header,
            data: data.to_vec(),
        });
    }
    result
}

pub fn group_aframes<T: Hash + Eq, F: Fn(&AfKey) -> Option<T>>(aframes: Vec<AdditionalFrame>, map_filter_fn: F) -> HashMap<T, Vec<AdditionalFrame>> {
    let mut result: HashMap<T, Vec<_>> = HashMap::new();
    for af in aframes {
        if let Some(key) = map_filter_fn(&af.key) {
            result.entry(key).or_default().push(af);
        }
    }
    result
}

pub struct AdditionalFrame {
    pub key: AfKey,
    pub header: AfHeader,
    pub data: Vec<u8>,
}

pub struct MessageBuilder<T: Serialize> {
    pub messages: Vec<T>,
    pub descriptor: AfDescriptor,
    pub data: Vec<Bytes>,
}

impl<T: Serialize> MessageBuilder<T> {
    pub fn new() -> Self {
        MessageBuilder {
            messages: Vec::new(),
            descriptor: Default::default(),
            data: Vec::new(),
        }
    }

    #[inline]
    pub fn add_message(&mut self, message: T) {
        self.messages.push(message);
    }

    pub fn add_frame(&mut self, mut key: AfKey, header: AfHeader, data: Vec<u8>, is_bytestring: bool) {
        key[0] = AfKeyElement::Index(self.messages.len() as u64);
        self.descriptor.keys.push(key.clone());
        if is_bytestring {
            self.descriptor.bytestrings.push(key.clone());
        }
        self.descriptor.headers.push((key, header));
        if self.data.is_empty() {
            // If first frame, reserve place for header
            self.data.push(Default::default());
        }
        self.data.push(data.into());
    }

    pub fn build(mut self) -> DaskMessage {
        let msg = rmp_serde::encode::to_vec_named(&self.messages).unwrap();
        if !self.data.is_empty() {
            let desc = rmp_serde::encode::to_vec_named(&self.descriptor).unwrap();
            self.data[0] = desc.into();
        }
        DaskMessage::new(msg.into(), self.data)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

/*
pub struct AfBuilder {
    descriptor: AfDescriptor,
    data: Vec<Bytes>,
}*/

/*
type AdditionalFramesMap = HashMap<u64, Vec<AdditionalFrame>>;


fn _to_map(value: rmpv::Value) -> Option<Vec<(Value, Value)>>
{
    match value {
        rmpv::Value::Map(m) => Some(m),
        _ => None
    }
}

fn _to_list(value: rmpv::Value) -> Option<Vec<Value>>
{
    match value {
        rmpv::Value::Array(a) => Some(a),
        _ => None
    }
}

pub fn merge_additional_frame_headers(mut headers: Vec<rmpv::Value>) -> rmpv::Value {
    let mut result : HashMap<rmpv::Value, Vec<rmpv::Value>>;
    for header in headers {
        for (k, v) in _to_map(header).unwrap() {
            result.entry(k).or_default().extend(_to_list(v).unwrap());
        }
    }
    rmpv::Value::Map(result.into_iter().map(|(k ,v)| (k, rmpv::Value::Array(v))).collect())
}


pub fn parse_additional_frames(mut additional_frames: Vec<Bytes>) -> crate::Result<()> {
    if additional_frames.is_empty() {
        return Ok(());
    }
    let headers = additional_frames.remove(0);
    let root =  rmpv::decode::value::read_value(&mut Cursor::new(headers)).unwrap(); // TODO error handling

        let headers = _to_map(root)
        .and_then(|mut map| {
            map.iter().position(|(key, value)| key.as_str() == Some("headers")).map(|p| map.swap_remove(p).1)
        }).and_then(|value| _to_map(value)).unwrap_or_else(|| {
            panic!("Cannot find valid headers");
        });

    let mut result: AdditionalFramesMap = Default::default();
    for ((key, value), data) in headers.into_iter().zip(additional_frames.into_iter()) {
        let position = key.as_array().and_then(|a| a.get(0)).and_then(|v| v.as_u64());
        if let Some(pos) = position {
            result.entry(pos).or_default().push(AdditionalFrame {
                header_key: key,
                header_value: value,
                data,
            });
        } else {
            // TODO: bail!
            panic!("Invalid position in headers");
        }
    }
    Ok(())
}
*/