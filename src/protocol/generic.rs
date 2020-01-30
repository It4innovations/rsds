use crate::scheduler::schedproto::WorkerId;
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize, Debug)]
pub struct IdentityMsg {
    //pub reply: bool,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Serialize, Debug)]
pub struct IdentityResponse {
    #[serde(rename = "type")]
    pub r#type: String,
    pub id: String,
    pub workers: Vec<WorkerId>,
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
