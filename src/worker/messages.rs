use serde::{Deserialize, Serialize};

use crate::common::data::SerializationType;
use crate::server::dask::key::DaskKey;

use super::subworker::SubworkerId;
use crate::scheduler::TaskId;

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId,
}

#[derive(Serialize, Debug)]
pub(crate) struct RegisterSubworkerResponse {
    pub(crate) worker: DaskKey,
}

#[derive(Serialize, Debug)]
pub struct UploadMsg {
    pub id: TaskId,
    pub serializer: SerializationType,
}

#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg<'a> {
    pub id: TaskId,

    #[serde(with = "serde_bytes")]
    pub spec: &'a Vec<u8>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "op")]
pub enum ToSubworkerMessage<'a> {
    ComputeTask(ComputeTaskMsg<'a>),
    Upload(UploadMsg),
}

#[derive(Deserialize, Debug)]
pub struct TaskFinishedMsg {
    pub id: TaskId,
    pub serializer: SerializationType,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Deserialize, Debug)]
pub struct TaskFailedMsg {
    pub id: TaskId,
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
