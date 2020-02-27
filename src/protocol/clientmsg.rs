use crate::protocol::protocol::{Frames, SerializedMemory, SerializedTransport, FromDaskTransport};

use crate::common::{Map, Priority};
use crate::protocol::key::DaskKey;
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ClientTaskSpec<T = SerializedMemory> {
    Direct {
        function: T,
        args: T,
        kwargs: Option<T>,
    },
    Serialized(T),
}

pub fn task_spec_to_memory(spec: ClientTaskSpec<SerializedTransport>, frames: &mut Frames) -> ClientTaskSpec<SerializedMemory> {
    match spec {
        ClientTaskSpec::Serialized(v) => {
            ClientTaskSpec::<SerializedMemory>::Serialized(
                v.to_memory(frames),
            )
        }
        ClientTaskSpec::Direct {
            function,
            args,
            kwargs,
        } => ClientTaskSpec::<SerializedMemory>::Direct {
            function: function.to_memory(frames),
            args: args.to_memory(frames),
            kwargs: kwargs.map(|v| v.to_memory(frames)),
        },
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
    pub frames: Frames
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

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum FromClientMessage {
    HeartbeatClient,
    UpdateGraph(UpdateGraphMsg),
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
                frames: std::mem::take(frames)
            }),
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
