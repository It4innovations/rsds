use serde::{Deserialize, Deserializer, Serialize};

use crate::common::Map;
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::{Frames, FromDaskTransport, SerializedMemory, SerializedTransport};
use crate::protocol::Priority;
use serde::de::Error;

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct DirectTaskSpec<T = SerializedMemory> {
    pub function: Option<T>,
    pub args: Option<T>,
    pub kwargs: Option<T>,
}

fn deserialize_task_spec<'de, D, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<DirectTaskSpec<T>, D::Error>
where
    D: Deserializer<'de>,
{
    let spec = DirectTaskSpec::<T>::deserialize(deserializer)?;
    if spec.function.is_none() && spec.args.is_none() && spec.kwargs.is_none() {
        Err(D::Error::custom("all fields are missing"))
    } else {
        Ok(spec)
    }
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ClientTaskSpec<T = SerializedMemory> {
    #[serde(deserialize_with = "deserialize_task_spec")]
    Direct(DirectTaskSpec<T>),
    Serialized(T),
}

pub fn task_spec_to_memory(
    spec: ClientTaskSpec<SerializedTransport>,
    frames: &mut Frames,
) -> ClientTaskSpec<SerializedMemory> {
    match spec {
        ClientTaskSpec::Serialized(v) => {
            ClientTaskSpec::<SerializedMemory>::Serialized(v.to_memory(frames))
        }
        ClientTaskSpec::Direct(DirectTaskSpec {
            function,
            args,
            kwargs,
        }) => ClientTaskSpec::<SerializedMemory>::Direct(DirectTaskSpec {
            function: function.map(|v| v.to_memory(frames)),
            args: args.map(|v| v.to_memory(frames)),
            kwargs: kwargs.map(|v| v.to_memory(frames)),
        }),
    }
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct UpdateGraphMsg {
    #[serde(with = "tuple_vec_map")]
    pub tasks: Vec<(DaskKey, ClientTaskSpec<SerializedTransport>)>,
    pub dependencies: Map<DaskKey, Vec<DaskKey>>,
    pub keys: Vec<DaskKey>,

    #[serde(default)]
    pub priority: Map<DaskKey, i32>,

    #[serde(default)]
    pub user_priority: Priority,

    pub actors: Option<bool>,

    #[serde(skip)]
    pub frames: Frames,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct ClientReleasesKeysMsg {
    pub keys: Vec<DaskKey>,
    pub client: DaskKey,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct ClientDesiresKeysMsg {
    pub keys: Vec<DaskKey>,
    pub client: DaskKey,
}

/** TASK ARRAY SUPPORT */
pub type Int = i32;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IntExpr {
    Index,
    Const(Int),
    Add(Box<(IntExpr, IntExpr)>),
    Mul(Box<(IntExpr, IntExpr)>),
    Div(Box<(IntExpr, IntExpr)>),
    Mod(Box<(IntExpr, IntExpr)>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RangeExpr {
    GetItem(IntExpr),
    // [x]
    Slice(IntExpr, IntExpr, IntExpr),
    // [start:end:step]
    All, // [:]
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArgumentExpr {
    Int(IntExpr),
    #[serde(with = "serde_bytes")]
    Serialized(Vec<u8>),
    Task(DaskKey),
    TaskArray(DaskKey, RangeExpr),
    //ObjectList(Vec<Vec<u8>>, RangeExpr),
}

#[derive(Debug, Deserialize)]
pub struct TaskArrayPart {
    pub size: Int,
    #[serde(with = "serde_bytes")]
    pub function: Vec<u8>,
    pub args: Vec<ArgumentExpr>,
}

#[derive(Debug, Deserialize)]
pub struct TaskArray {
    pub key: DaskKey,
    pub parts: Vec<TaskArrayPart>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateArrayGraphMsg {
    pub arrays: Vec<TaskArray>,
}

/** END OF TASK ARRAY SUPPORT */

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum FromClientMessage {
    HeartbeatClient,
    UpdateGraph(UpdateGraphMsg),
    UpdateArrayGraph(UpdateArrayGraphMsg),
    ClientReleasesKeys(ClientReleasesKeysMsg),
    ClientDesiresKeys(ClientDesiresKeysMsg),
    CloseClient,
    CloseStream,
}

impl FromDaskTransport for FromClientMessage {
    type Transport = Self;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self {
        match source {
            Self::Transport::HeartbeatClient => Self::HeartbeatClient,
            Self::Transport::UpdateGraph(data) => Self::UpdateGraph(UpdateGraphMsg {
                tasks: data.tasks,
                dependencies: data.dependencies,
                keys: data.keys,
                actors: data.actors,
                priority: data.priority,
                user_priority: data.user_priority,
                frames: std::mem::take(frames),
            }),
            Self::Transport::UpdateArrayGraph(data) => Self::UpdateArrayGraph(data),
            Self::Transport::ClientReleasesKeys(data) => Self::ClientReleasesKeys(data),
            Self::Transport::ClientDesiresKeys(data) => Self::ClientDesiresKeys(data),
            Self::Transport::CloseClient => Self::CloseClient,
            Self::Transport::CloseStream => Self::CloseStream,
        }
    }
}

#[cfg_attr(test, derive(Deserialize, PartialEq))]
#[derive(Serialize, Debug)]
pub struct KeyInMemoryMsg {
    pub key: DaskKey,
    #[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,
}

#[cfg_attr(test, derive(Deserialize, PartialEq))]
#[derive(Serialize, Debug)]
pub struct TaskErredMsg {
    pub key: DaskKey,
    pub exception: SerializedTransport,
    pub traceback: SerializedTransport,
}

#[cfg_attr(test, derive(Deserialize, PartialEq))]
#[derive(Serialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ToClientMessage {
    KeyInMemory(KeyInMemoryMsg),
    TaskErred(TaskErredMsg),
}
from_dask_transport!(test, ToClientMessage);
