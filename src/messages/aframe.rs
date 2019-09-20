use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
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
}

type AfKey = Vec<AfKeyElement>;

/*#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum AfKey {
    Single(AfKeyElement),
    Composed(Vec<AfKeyElement>),
}*/

#[derive(Serialize, Deserialize, Debug)]
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
    pub bytestrings: Vec<()>, // Expected type?
}

impl AfDescriptor {
    pub fn split_frames_by_index(self, additional_headers: Vec<Bytes>) -> crate::Result<HashMap<u64, Vec<AdditionalFrame>>> {
        let mut result: HashMap<u64, Vec<AdditionalFrame>> = HashMap::new();
        for ((key, header), data) in self.headers.into_iter().zip(additional_headers) {
            if key.is_empty() {
                panic!("Key is empty"); // TODO: bail
            }
            let index = key[0].as_index().unwrap_or_else(|| {
                panic!("FIrst element of key is not index"); // TODO bail
            });
            result.entry(index).or_default().push(AdditionalFrame {
                header_key: key,
                header_value: header,
                data,
            });
        }
        Ok(result)
    }
}


pub struct AdditionalFrame {
    pub header_key: AfKey,
    pub header_value: AfHeader,
    pub data: Bytes,
}
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