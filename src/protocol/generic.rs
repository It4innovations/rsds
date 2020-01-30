use crate::common::Map;
use crate::protocol::protocol::{
    map_from_transport, map_to_transport, Frames, FromDaskTransport, MessageBuilder,
    SerializedMemory, SerializedTransport, ToDaskTransport,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct IdentityMsg {
    //pub reply: bool,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct BandwidthInfo {
    pub total: u64,
    pub types: Map<String, String>,
    pub workers: Map<String, u64>,
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
    pub host: String,
    pub id: String,
    pub last_seen: f64,
    pub local_directory: String,
    pub memory_limit: u64,
    pub metrics: WorkerMetrics,
    pub name: String,
    pub nanny: String,
    pub nthreads: u64,
    pub resources: Map<String, String>,
    pub services: Map<String, u64>,
    pub r#type: String,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct IdentityResponse {
    #[serde(rename = "type")]
    pub r#type: String,
    pub id: String,
    pub workers: Map<String, WorkerInfo>,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct RegisterClientMsg {
    pub client: String,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: String,
    /*pub nthreads: u32,
    pub memorylimit: u64,
    pub nanny: String,*/
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HeartbeatWorkerMsg {
    pub now: f64,
    // TODO
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct GatherMsg {
    pub keys: Vec<String>,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct ScatterMsg<T = SerializedMemory> {
    pub client: String,
    pub broadcast: bool,
    pub data: Map<String, T>,
    pub reply: bool,
    pub timeout: u64,
    pub workers: Option<Vec<String>>,
}

pub type ScatterResponse = Vec<String>;

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct CancelKeysMsg {
    keys: Vec<String>,
    client: String,
    force: bool,
    reply: bool,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum GenericMessage<T = SerializedMemory> {
    Identity(IdentityMsg),
    #[serde(rename = "heartbeat_worker")]
    HeartbeatWorker(HeartbeatWorkerMsg),
    RegisterClient(RegisterClientMsg),
    RegisterWorker(RegisterWorkerMsg),
    Gather(GatherMsg),
    Scatter(ScatterMsg<T>),
    Cancel(CancelKeysMsg),
    Ncores,
}

impl FromDaskTransport for GenericMessage<SerializedMemory> {
    type Transport = GenericMessage<SerializedTransport>;

    fn to_memory(source: Self::Transport, frames: &mut Frames) -> Self {
        match source {
            Self::Transport::Identity(msg) => Self::Identity(msg),
            Self::Transport::HeartbeatWorker(msg) => Self::HeartbeatWorker(msg),
            Self::Transport::RegisterClient(msg) => Self::RegisterClient(msg),
            Self::Transport::RegisterWorker(msg) => Self::RegisterWorker(msg),
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
        }
    }
}

#[cfg(test)]
impl ToDaskTransport for GenericMessage<SerializedMemory> {
    type Transport = GenericMessage<SerializedTransport>;

    fn to_transport(self, message_builder: &mut MessageBuilder<Self::Transport>) {
        match self {
            Self::Identity(msg) => Self::Transport::Identity(msg),
            Self::HeartbeatWorker(msg) => Self::Transport::HeartbeatWorker(msg),
            Self::RegisterClient(msg) => Self::Transport::RegisterClient(msg),
            Self::RegisterWorker(msg) => Self::Transport::RegisterWorker(msg),
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
        };
    }
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct SimpleMessage {
    pub op: String,
}
