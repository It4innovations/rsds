use crate::common::Map;
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::{
    map_from_transport, map_to_transport, Frames, FromDaskTransport, MessageBuilder,
    SerializedMemory, SerializedTransport, ToDaskTransport,
};
use crate::protocol::{Float, Priority};
use serde::{Deserialize, Serialize, Serializer};

fn binary_is_empty(transport: &Option<SerializedTransport>) -> bool {
    match transport {
        Some(transport) => match transport {
            SerializedTransport::Indexed { .. } => false,
            SerializedTransport::Inline(v) => match v {
                rmpv::Value::Binary(v) => v.is_empty(),
                _ => false,
            },
        },
        None => false,
    }
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub key: DaskKey,
    pub duration: f32, // estimated duration, [in seconds?]

    #[serde(skip_serializing_if = "bool_is_false")]
    #[serde(default)]
    pub actor: bool,

    #[serde(with = "tuple_vec_map")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub who_has: Vec<(DaskKey, Vec<DaskKey>)>,

    #[serde(with = "tuple_vec_map")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub nbytes: Vec<(DaskKey, u64)>,

    #[serde(skip_serializing_if = "binary_is_empty")]
    pub function: Option<SerializedTransport>,

    #[serde(skip_serializing_if = "binary_is_empty")]
    pub args: Option<SerializedTransport>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<SerializedTransport>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<SerializedTransport>,

    #[serde(default)]
    pub priority: [Priority; 3],
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteDataMsg {
    pub keys: Vec<DaskKey>,
    pub report: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetDataMsg {
    pub keys: Vec<DaskKey>,
    pub who: Option<u64>,
    // ?
    pub max_connections: bool,
    // ?
    pub reply: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateDataMsg {
    pub data: Map<DaskKey, SerializedTransport>,
    pub reply: bool,
    pub report: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StealRequestMsg {
    pub key: DaskKey,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ToWorkerMessage {
    ComputeTask(ComputeTaskMsg),
    DeleteData(DeleteDataMsg),
    #[serde(rename = "get_data")]
    GetData(GetDataMsg),
    #[serde(rename = "update_data")]
    UpdateData(UpdateDataMsg),
    StealRequest(StealRequestMsg),
}
from_dask_transport!(ToWorkerMessage);

// Worker side protocol
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ToWorkerStreamMessage {
    ComputeTask(ComputeTaskMsg),
    DeleteData(DeleteDataMsg),
    StealRequest(StealRequestMsg),
}
from_dask_transport!(ToWorkerStreamMessage);

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ToWorkerGenericMessage {
    #[serde(rename = "get_data")]
    GetData(GetDataMsg),
    UpdateData(UpdateDataMsg),
    DeleteData(DeleteDataMsg),
}
from_dask_transport!(ToWorkerGenericMessage);

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterWorkerResponseMsg {
    pub status: DaskKey,
    pub time: f64,
    pub heartbeat_interval: Float,
    pub worker_plugins: Vec<()>, // type of plugins??
}
from_dask_transport!(RegisterWorkerResponseMsg);

// FIX: Deserialize from string (does it working for msgpack??)
#[derive(Deserialize, Debug, PartialEq)]
pub enum Status {
    #[serde(rename = "OK")]
    Ok,
    #[serde(rename = "error")]
    Error,
}

impl Serialize for Status {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            Self::Ok => "OK",
            Self::Error => "error",
        })
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub status: Status,
    pub key: DaskKey,
    pub nbytes: u64,
    #[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,
    pub startstops: Vec<Map<DaskKey, rmpv::Value>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskErredMsg<T = SerializedMemory> {
    pub status: Status,
    pub key: DaskKey,
    pub thread: u64,
    pub exception: T,
    pub traceback: T,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddKeysMsg {
    pub keys: Vec<DaskKey>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerState {
    Waiting,
    Ready,
    Executing,
    Memory,
    Error,
    Rescheduled,
    Constrained,
    LongRunning,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StealResponseMsg {
    pub key: DaskKey,
    pub state: Option<WorkerState>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReleaseMsg {
    pub key: DaskKey,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum FromWorkerMessage<T = SerializedMemory> {
    TaskFinished(TaskFinishedMsg),
    TaskErred(TaskErredMsg<T>),
    AddKeys(AddKeysMsg),
    KeepAlive,
    Unregister,
    StealResponse(StealResponseMsg),
    Release(ReleaseMsg),
    CloseStream,
}

impl FromDaskTransport for FromWorkerMessage<SerializedMemory> {
    type Transport = FromWorkerMessage<SerializedTransport>;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self {
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
            Self::Transport::StealResponse(msg) => Self::StealResponse(msg),
            Self::Transport::Release(msg) => Self::Release(msg),
            Self::Transport::CloseStream => Self::CloseStream,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Empty;

#[derive(Deserialize, Serialize, Debug)]
pub struct GetDataResponse<T = SerializedMemory> {
    pub status: DaskKey, // TODO: Migrate to enum Status
    pub data: Map<DaskKey, T>,
}

impl FromDaskTransport for GetDataResponse<SerializedMemory> {
    type Transport = GetDataResponse<SerializedTransport>;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateDataResponse {
    pub status: DaskKey,
    pub nbytes: Map<DaskKey, u64>,
}
from_dask_transport!(UpdateDataResponse);
