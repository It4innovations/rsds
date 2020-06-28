use crate::protocol::key::DaskKey;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: DaskKey,
    pub ncpus: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum GenericMessage {
    RegisterWorker(RegisterWorkerMsg),
}