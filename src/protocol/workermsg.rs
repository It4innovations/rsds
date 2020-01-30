use crate::common::Map;
use crate::protocol::protocol::{
    map_from_transport, map_to_transport, Frames, FromDaskTransport, MessageBuilder,
    SerializedMemory, SerializedTransport, ToDaskTransport,
};
use serde::{Deserialize, Serialize};

/*
{b'status': b'OK',
 b'time': 1568021833.7372987,
 b'heartbeat-interval': 0.5,
 b'worker-plugins': []}

[{b'op': b'compute-task',
  b'key': b'do_something-8d2f1b8d-bbff-423c-a2d1-157ed71d4bd1',
  b'priority': [0, 1, 0],
  b'duration': 0.5,
  b'function': b'\x80\x04\x95\\\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x02KCC\x08|\x00d\x01\x14\x00S\x00\x94NK\n\x86\x94)\x8c\x01x\x94\x85\x94\x8c\tclient.py\x94\x8c\x0cdo_something\x94K\x07C\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x16\x8c\x04name\x94h\x0f\x8c\x03doc\x94N\x8c\x08qualname\x94h\x0futR.',
  b'args': b'\x80\x04\x95\x05\x00\x00\x00\x00\x00\x00\x00K\n\x85\x94.'},
 {b'op': b'compute-task',
  b'key': b'do_something-ab6571c5-631d-4fb5-97f5-8b3583b7513a',
  b'priority': [0, 1, 1],
  b'duration': 0.5,
  b'function': b'\x80\x04\x95\\\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x02KCC\x08|\x00d\x01\x14\x00S\x00\x94NK\n\x86\x94)\x8c\x01x\x94\x85\x94\x8c\tclient.py\x94\x8c\x0cdo_something\x94K\x07C\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x16\x8c\x04name\x94h\x0f\x8c\x03doc\x94N\x8c\x08qualname\x94h\x0futR.',
  b'args': b'\x80\x04\x95\x05\x00\x00\x00\x00\x00\x00\x00K\x03\x85\x94.'}]

{b'op': b'compute-task',
  b'key': b'adder-1a067e76-eba6-4bb9-afb3-42a5efd2aea9',
  b'priority': [0, 1, 2],
  b'duration': 0.5,
  b'who_has': {b'do_something-8d2f1b8d-bbff-423c-a2d1-157ed71d4bd1': [b'tcp://127.0.0.1:37849'],
   b'do_something-ab6571c5-631d-4fb5-97f5-8b3583b7513a': [b'tcp://127.0.0.1:37849']},
  b'nbytes': {b'do_something-8d2f1b8d-bbff-423c-a2d1-157ed71d4bd1': 28,
   b'do_something-ab6571c5-631d-4fb5-97f5-8b3583b7513a': 28},
  b'function': b'\x80\x04\x95_\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x03K\x00K\x03K\x02KCC\x0c|\x00|\x01\x17\x00|\x02\x17\x00S\x00\x94N\x85\x94)\x8c\x01x\x94\x8c\x01y\x94\x8c\x01z\x94\x87\x94\x8c\tclient.py\x94\x8c\x05adder\x94K\x0bC\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x18\x8c\x04name\x94h\x11\x8c\x03doc\x94N\x8c\x08qualname\x94h\x11utR.',
  b'args': b'\x80\x04\x95m\x00\x00\x00\x00\x00\x00\x00\x8c1do_something-8d2f1b8d-bbff-423c-a2d1-157ed71d4bd1\x94\x8c1do_something-ab6571c5-631d-4fb5-97f5-8b3583b7513a\x94K\x03\x87\x94.'}

[{b'op': b'task-finished',
  b'status': b'OK',
  b'key': b'do_something-8d2f1b8d-bbff-423c-a2d1-157ed71d4bd1',
  b'nbytes': 28,
  b'thread': 140500728280832,
  b'type': b'\x80\x04\x95\x14\x00\x00\x00\x00\x00\x00\x00\x8c\x08builtins\x94\x8c\x03int\x94\x93\x94.',
  b'typename': b'builtins.int',
  b'startstops': [[b'compute', 1568037973.9604871, 1568037973.96052]]},
 {b'op': b'task-finished',
  b'status': b'OK',
  b'key': b'do_something-ab6571c5-631d-4fb5-97f5-8b3583b7513a',
  b'nbytes': 28,
  b'thread': 140500728280832,
  b'type': b'\x80\x04\x95\x14\x00\x00\x00\x00\x00\x00\x00\x8c\x08builtins\x94\x8c\x03int\x94\x93\x94.',
  b'typename': b'builtins.int',
  b'startstops': [[b'compute', 1568037973.960996, 1568037973.961003]]}]

{b'op': b'task-finished',
  b'status': b'OK',
  b'key': b'do_something-564ab5c2-4721-4cc7-a546-29fd8ec7ccee',
  b'nbytes': 28,
  b'thread': 139932270024448,
  b'type': b'\x80\x04\x95\x14\x00\x00\x00\x00\x00\x00\x00\x8c\x08builtins\x94\x8c\x03int\x94\x93\x94.',
  b'typename': b'builtins.int',
  b'startstops': [[b'compute', 10.7310631275177, 10.731109619140625]]}

[{b'op': b'task-erred',
  b'status': b'error',
  b'key': b'adder-f755a72a-f1c0-4bde-97da-8f2ed81b300f',
  b'thread': 140195060229888,
  b'startstops': [[b'compute', 1.337095022201538, 1.3390309810638428]]}]

get_data response:
b"\x82\xa6status\xa2OK\xa4data\x80"
[b"\x83\xa7headers\x81\x92\xa4data\xd9#fn-290914ef7acde00ce350415c5821f1d4\x84\xaaserializer\xa6pickle\xa7lengths\x91\x0e\xabcompression\x91\xc0\xa5count\x01\xa4keys\x91\x92\xa4data\xd9#fn-290914ef7acde00ce350415c5821f1d4\xabbytestrings\x90", b"\x80\x04\x95\x03\0\0\0\0\0\0\0K\x02."]

*/

fn binary_is_empty(transport: &SerializedTransport) -> bool {
    match transport {
        SerializedTransport::Indexed { .. } => false,
        SerializedTransport::Inline(v) => match v {
            rmpv::Value::Binary(v) => v.is_empty(),
            _ => false,
        },
    }
}

#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg {
    pub key: String,
    pub duration: f32, // estimated duration, [in seconds?]

    #[serde(with = "tuple_vec_map")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub who_has: Vec<(String, Vec<String>)>,

    #[serde(with = "tuple_vec_map")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub nbytes: Vec<(String, u64)>,

    #[serde(skip_serializing_if = "binary_is_empty")]
    pub function: SerializedTransport,

    #[serde(skip_serializing_if = "binary_is_empty")]
    pub args: SerializedTransport,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<SerializedTransport>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<SerializedTransport>,
}

#[derive(Serialize, Debug)]
pub struct DeleteDataMsg {
    pub keys: Vec<String>,
    pub report: bool,
}

#[derive(Serialize, Debug)]
pub struct GetDataMsg<'a> {
    pub keys: &'a [&'a str],
    pub who: Option<u64>,
    // ?
    pub max_connections: bool,
    // ?
    pub reply: bool,
}

#[derive(Serialize, Debug)]
pub struct UpdateDataMsg {
    pub data: Map<String, SerializedTransport>,
    pub reply: bool,
    pub report: bool,
}

#[derive(Serialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ToWorkerMessage<'a> {
    ComputeTask(ComputeTaskMsg),
    DeleteData(DeleteDataMsg),
    #[serde(rename = "get_data")]
    GetData(GetDataMsg<'a>),
    #[serde(rename = "update_data")]
    UpdateData(UpdateDataMsg),
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterWorkerResponseMsg {
    pub status: String,
    pub time: f64,
    pub heartbeat_interval: f64,
    pub worker_plugins: Vec<()>, // type of plugins??
}

// FIX: Deserialize from string (does it working for msgpack??)
#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub enum Status {
    #[serde(rename = "OK")]
    Ok,
    #[serde(rename = "error")]
    Error,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct TaskFinishedMsg {
    pub status: Status,
    pub key: String,
    pub nbytes: u64,

    #[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct TaskErredMsg<T = SerializedMemory> {
    pub status: Status,
    pub key: String,
    pub thread: u64,
    pub exception: T,
    pub traceback: T,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct AddKeysMsg {
    pub keys: Vec<String>,
}

/*#[derive(Deserialize, Debug)]
pub struct RemoveKeysMsg {
    // It seems that it just informative message, ignoring
}

    #[serde(rename = "remove_keys")]
    RemoveKeys(RemoveKeysMsg),*/

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum FromWorkerMessage<T = SerializedMemory> {
    TaskFinished(TaskFinishedMsg),
    TaskErred(TaskErredMsg<T>),
    AddKeys(AddKeysMsg),
    KeepAlive,
    Unregister,
}

impl FromDaskTransport for FromWorkerMessage<SerializedMemory> {
    type Transport = FromWorkerMessage<SerializedTransport>;

    fn to_memory(source: Self::Transport, frames: &mut Frames) -> Self {
        match source {
            Self::Transport::TaskFinished(msg) => Self::TaskFinished(msg),
            Self::Transport::TaskErred(msg) => Self::TaskErred(TaskErredMsg {
                status: msg.status,
                key: msg.key,
                thread: msg.thread,
                exception: msg.exception.to_memory(frames),
                traceback: msg.traceback.to_memory(frames),
            }),
            Self::Transport::AddKeys(msg) => Self::AddKeys(msg),
            Self::Transport::KeepAlive => Self::KeepAlive,
            Self::Transport::Unregister => Self::Unregister,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Empty;

#[derive(Deserialize, Serialize, Debug)]
pub struct GetDataResponse<T = SerializedMemory> {
    pub status: String, // TODO: Migrate to enum Status
    pub data: Map<String, T>,
}

impl FromDaskTransport for GetDataResponse<SerializedMemory> {
    type Transport = GetDataResponse<SerializedTransport>;

    fn to_memory(source: Self::Transport, frames: &mut Frames) -> Self {
        GetDataResponse {
            status: source.status,
            data: map_from_transport(source.data, frames),
        }
    }
}
impl ToDaskTransport for GetDataResponse<SerializedMemory> {
    type Transport = GetDataResponse<SerializedTransport>;

    fn to_transport(self, message_builder: &mut MessageBuilder<Self::Transport>) {
        let msg = GetDataResponse {
            status: self.status,
            data: map_to_transport(self.data, message_builder),
        };
        message_builder.add_message(msg);
    }
}

#[derive(Deserialize, Debug)]
pub struct UpdateDataResponse {
    pub status: String,
    pub nbytes: Map<String, u64>
}
