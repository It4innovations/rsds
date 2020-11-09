use serde::{Deserialize, Serialize};

use crate::common::Map;
use crate::server::protocol::dasktransport::{
    map_from_transport, Frames, FromDaskTransport, SerializedMemory, SerializedTransport,
};
use crate::server::protocol::key::DaskKey;

#[derive(Serialize, Deserialize, Debug)]
pub struct IdentityMsg {
    //pub reply: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct BandwidthInfo {
    pub total: u64,
    pub types: Map<DaskKey, DaskKey>,
    pub workers: Map<DaskKey, u64>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct WorkerMetrics {
    pub bandwidth: BandwidthInfo,
    pub executing: u64,
    pub in_flight: u64,
    pub in_memory: u64,
    pub memory: u64,
    pub num_fds: u64,
    pub read_bytes: f64,
    pub ready: u64,
    pub time: f64,
    pub write_bytes: f64,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub host: DaskKey,
    pub id: DaskKey,
    pub last_seen: f64,
    pub local_directory: DaskKey,
    pub memory_limit: u64,
    pub metrics: WorkerMetrics,
    pub name: DaskKey,
    pub nanny: DaskKey,
    pub nthreads: u64,
    pub resources: Map<DaskKey, DaskKey>,
    pub services: Map<DaskKey, u64>,
    pub r#type: DaskKey,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct IdentityResponse {
    #[serde(rename = "type")]
    pub r#type: DaskKey,
    pub id: DaskKey,
    pub workers: Map<DaskKey, WorkerInfo>,
}

from_dask_transport!(test, IdentityResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterClientMsg {
    pub client: DaskKey,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub name: String,
    pub address: DaskKey,
    pub nthreads: u32,
    /*pub memorylimit: u64,
    pub nanny: String,*/
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HeartbeatWorkerMsg {
    pub now: f64,
    // TODO
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct GatherMsg {
    pub keys: Vec<DaskKey>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct ScatterMsg<T = SerializedMemory> {
    pub client: DaskKey,
    pub broadcast: bool,
    pub data: Map<DaskKey, T>,
    pub reply: bool,
    pub timeout: u64,
    pub workers: Option<Vec<DaskKey>>,
}

pub type ScatterResponse = Vec<DaskKey>;

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelKeysMsg {
    keys: Vec<DaskKey>,
    client: DaskKey,
    force: bool,
    reply: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WhoHasMsg {
    pub keys: Option<Vec<DaskKey>>,
}

pub type WhoHasMsgResponse = Map<DaskKey, Vec<DaskKey>>; // key -> [worker address]

#[derive(Serialize, Deserialize, Debug)]
pub struct ProxyMsg {
    pub worker: DaskKey,
    pub msg: rmpv::Value,
    #[serde(skip)]
    pub frames: Frames,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum GenericMessage<T = SerializedMemory> {
    Identity(IdentityMsg),
    #[serde(rename = "heartbeat_worker")]
    HeartbeatWorker(HeartbeatWorkerMsg),
    RegisterClient(RegisterClientMsg),
    RegisterWorker(RegisterWorkerMsg),
    #[serde(rename = "who_has")]
    WhoHas(WhoHasMsg),
    Gather(GatherMsg),
    Scatter(ScatterMsg<T>),
    Cancel(CancelKeysMsg),
    Ncores,
    Proxy(ProxyMsg),
    Unregister,
}

impl FromDaskTransport for GenericMessage<SerializedMemory> {
    type Transport = GenericMessage<SerializedTransport>;

    fn deserialize(source: Self::Transport, frames: &mut Frames) -> Self {
        match source {
            Self::Transport::Identity(msg) => Self::Identity(msg),
            Self::Transport::HeartbeatWorker(msg) => Self::HeartbeatWorker(msg),
            Self::Transport::RegisterClient(msg) => Self::RegisterClient(msg),
            Self::Transport::RegisterWorker(msg) => Self::RegisterWorker(msg),
            Self::Transport::WhoHas(msg) => Self::WhoHas(msg),
            Self::Transport::Gather(msg) => Self::Gather(msg),
            Self::Transport::Scatter(msg) => Self::Scatter(ScatterMsg {
                client: msg.client,
                broadcast: msg.broadcast,
                data: map_from_transport(msg.data, frames),
                reply: msg.reply,
                timeout: msg.timeout,
                workers: msg.workers,
            }),
            Self::Transport::Cancel(msg) => Self::Cancel(msg),
            Self::Transport::Ncores => Self::Ncores,
            Self::Transport::Proxy(msg) => Self::Proxy(ProxyMsg {
                worker: msg.worker,
                msg: msg.msg,
                frames: std::mem::take(frames),
            }),
            Self::Transport::Unregister => Self::Unregister,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SimpleMessage {
    pub op: DaskKey,
}
from_dask_transport!(test, SimpleMessage);

#[cfg(test)]
mod tests {
    use crate::server::protocol::daskmessages::generic::{GenericMessage, ScatterMsg};
    use crate::server::protocol::dasktransport::{
        map_to_transport, MessageBuilder, SerializedMemory, SerializedTransport, ToDaskTransport,
    };

    impl ToDaskTransport for GenericMessage<SerializedMemory> {
        type Transport = GenericMessage<SerializedTransport>;

        fn to_transport(self, message_builder: &mut MessageBuilder<Self::Transport>) {
            match self {
                Self::Identity(msg) => Self::Transport::Identity(msg),
                Self::HeartbeatWorker(msg) => Self::Transport::HeartbeatWorker(msg),
                Self::RegisterClient(msg) => Self::Transport::RegisterClient(msg),
                Self::RegisterWorker(msg) => Self::Transport::RegisterWorker(msg),
                Self::WhoHas(msg) => Self::Transport::WhoHas(msg),
                Self::Gather(msg) => Self::Transport::Gather(msg),
                Self::Scatter(msg) => Self::Transport::Scatter(ScatterMsg {
                    client: msg.client,
                    broadcast: msg.broadcast,
                    data: map_to_transport(msg.data, message_builder),
                    reply: msg.reply,
                    timeout: msg.timeout,
                    workers: msg.workers,
                }),
                Self::Cancel(msg) => Self::Transport::Cancel(msg),
                Self::Ncores => Self::Transport::Ncores,
                Self::Proxy(msg) => Self::Transport::Proxy(msg),
                Self::Unregister => Self::Transport::Unregister,
            };
        }
    }
}
