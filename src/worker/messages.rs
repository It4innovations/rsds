use serde::{Deserialize, Serialize};

use crate::common::data::SerializationType;
use crate::server::protocol::key::DaskKey;

use super::subworker::SubworkerId;

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId,
}

#[derive(Serialize, Debug)]
pub(crate) struct RegisterSubworkerResponse {
    pub(crate) worker: DaskKey,
}

#[derive(Serialize, Debug)]
pub struct Upload {
    pub key: DaskKey,
    pub serializer: SerializationType,
}

#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg<'a> {
    pub key: &'a DaskKey,

    pub function: &'a rmpv::Value,
    pub args: &'a rmpv::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: &'a Option<rmpv::Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub uploads: Vec<Upload>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "op")]
pub enum ToSubworkerMessage<'a> {
    ComputeTask(ComputeTaskMsg<'a>),
}

#[derive(Deserialize, Debug)]
pub struct TaskFinishedMsg {
    pub key: DaskKey,
    pub serializer: SerializationType,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Deserialize, Debug)]
pub struct TaskFailedMsg {
    pub key: DaskKey,
    #[serde(with = "serde_bytes")]
    pub exception: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub traceback: Vec<u8>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromSubworkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
}
