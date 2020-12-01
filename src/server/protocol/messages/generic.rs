use serde::{Deserialize, Serialize};

use crate::server::dask::key::DaskKey;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: String,
    pub ncpus: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum GenericMessage {
    RegisterWorker(RegisterWorkerMsg),
}
