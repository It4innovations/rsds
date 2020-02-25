use crate::protocol::protocol::{Frames, FromDaskTransport, SerializedMemory, SerializedTransport};

use crate::common::Map;
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

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct UpdateGraphMsg<T = SerializedMemory> {
    pub tasks: Map<DaskKey, ClientTaskSpec<T>>,
    pub dependencies: Map<DaskKey, Vec<DaskKey>>,
    pub keys: Vec<DaskKey>,
    pub actors: Option<bool>
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
pub enum FromClientMessage<T = SerializedMemory> {
    HeartbeatClient,
    UpdateGraph(UpdateGraphMsg<T>),
    ClientReleasesKeys(ClientReleasesKeysMsg),
    ClientDesiresKeys(ClientDesiresKeysMsg),
    CloseClient,
    CloseStream,
}

impl FromDaskTransport for FromClientMessage<SerializedMemory> {
    type Transport = FromClientMessage<SerializedTransport>;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self {
        match source {
            Self::Transport::HeartbeatClient => Self::HeartbeatClient,
            Self::Transport::UpdateGraph(data) => Self::UpdateGraph(UpdateGraphMsg {
                tasks: data
                    .tasks
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            match v {
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
                            },
                        )
                    })
                    .collect(),
                dependencies: data.dependencies,
                keys: data.keys,
                actors: data.actors
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
