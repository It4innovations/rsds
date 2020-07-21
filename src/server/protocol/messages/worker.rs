use serde::{Serialize, Deserialize};
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::Priority;


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
    ComputeTask(ComputeTaskMsg)
}