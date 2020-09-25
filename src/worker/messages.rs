use super::subworker::SubworkerId;
use crate::server::protocol::key::DaskKey;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId,
}

#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg<'a> {
    pub key: &'a DaskKey,

    pub function: &'a rmpv::Value,
    pub args: &'a rmpv::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub kwargs: &'a Option<rmpv::Value>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "op")]
pub enum ToSubworkerMessage<'a> {
    ComputeTask(ComputeTaskMsg<'a>),
}


#[derive(Deserialize, Debug)]
pub struct TaskFinishedMsg {
    pub key: DaskKey,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Deserialize, Debug)]
pub struct TaskErroredMsg {
    pub key: DaskKey,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromSubworkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskErrored(TaskErroredMsg),
}