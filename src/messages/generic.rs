use serde::{Deserialize, Serialize};

use crate::task::TaskKey;

#[derive(Deserialize, Debug)]
pub struct IdentityMsg {
    //pub reply: bool,
}

#[derive(Serialize, Debug)]
pub struct IdentityResponse {
    #[serde(rename = "type")]
    pub i_type: &'static str,
    pub id: String,
}

#[derive(Deserialize, Debug)]
pub struct RegisterClientMsg {
    pub client: String,
}

#[derive(Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: String,
    /*pub nthreads: u32,
    pub memorylimit: u64,
    pub nanny: String,*/
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct HeartbeatWorkerMsg {
    pub now: f64,
    // TODO
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct GatherMsg {
    pub keys: Vec<String>
}

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
}

#[derive(Serialize, Debug)]
pub struct SimpleMessage {
    pub op: &'static str,
}

/*#[derive(Serialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum GenericResponse {
    StreamStart(EmptyStruct),
    StreamStart2(EmptyStruct),
}*/

/*
#[derive(Serialize, Deserialize, Debug)]
pub struct AdditionalFrameHeaders {
    #[serde(with = "tuple_vec_map")]
    headers: Vec<(Value, Value)>,
    keys: Vec<Value>,
}*/