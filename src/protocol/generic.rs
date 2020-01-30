use crate::common::Map;
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
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum GenericMessage {
    Identity(IdentityMsg),
    #[serde(rename = "heartbeat_worker")]
    HeartbeatWorker(HeartbeatWorkerMsg),
    RegisterClient(RegisterClientMsg),
    RegisterWorker(RegisterWorkerMsg),
    Gather(GatherMsg),
    Ncores,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct SimpleMessage {
    pub op: String,
}
