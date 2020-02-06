use crate::common::Map;
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::{
    map_from_transport, Frames, FromDaskTransport, SerializedMemory, SerializedTransport,
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

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct RegisterClientMsg {
    pub client: DaskKey,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: DaskKey,
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
    pub keys: Vec<DaskKey>,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
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

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct CancelKeysMsg {
    keys: Vec<DaskKey>,
    client: DaskKey,
    force: bool,
    reply: bool,
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct WhoHasMsg {
    pub keys: Vec<DaskKey>,
}

pub type WhoHasMsgResponse = Map<DaskKey, Vec<DaskKey>>; // key -> [worker address]

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
    #[serde(rename = "who_has")]
    WhoHas(WhoHasMsg),
    Gather(GatherMsg),
    Scatter(ScatterMsg<T>),
    Cancel(CancelKeysMsg),
    Ncores,
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
        }
    }
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct SimpleMessage {
    pub op: DaskKey,
}

#[cfg(test)]
mod tests {
    use crate::protocol::generic::{GenericMessage, ScatterMsg};
    use crate::protocol::protocol::{
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
            };
        }
    }
}
