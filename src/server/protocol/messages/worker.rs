use crate::server::protocol::key::DaskKey;
use crate::server::protocol::Priority;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub key: DaskKey,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dep_info: Vec<(DaskKey, Vec<DaskKey>)>,

    pub function: rmpv::Value,

    pub args: rmpv::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub kwargs: Option<rmpv::Value>,

    pub user_priority: Priority,
    pub scheduler_priority: Priority,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum ToWorkerMessage {
    ComputeTask(ComputeTaskMsg),
}


#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub key: DaskKey,
    pub nbytes: u64,
    #[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromWorkerMessage {
    TaskFinished(TaskFinishedMsg),
}