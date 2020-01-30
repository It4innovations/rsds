use crate::common::Map;
use crate::protocol::protocol::{
    map_from_transport, map_to_transport, Frames, FromDaskTransport, MessageBuilder,
    SerializedMemory, SerializedTransport, ToDaskTransport,
};
use serde::{Deserialize, Serialize};

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
    pub nbytes: Map<String, u64>,
}
