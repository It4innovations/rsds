use crate::server::protocol::key::DaskKey;
use crate::server::protocol::PriorityValue;
use serde::{Deserialize, Serialize};
use crate::common::data::SerializationType;

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub key: DaskKey,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dep_info: Vec<(DaskKey, u64, Vec<DaskKey>)>,

    pub function: rmpv::Value,

    pub args: rmpv::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub kwargs: Option<rmpv::Value>,

    pub user_priority: PriorityValue,
    pub scheduler_priority: PriorityValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteDataMsg {
    pub keys: Vec<DaskKey>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum ToWorkerMessage {
    ComputeTask(ComputeTaskMsg),
    DeleteData(DeleteDataMsg),
}


#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub key: DaskKey,
    pub nbytes: u64,
    /*#[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,*/
}


#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFailedMsg {
    pub key: DaskKey,
    #[serde(with = "serde_bytes")]
    pub exception: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub traceback: Vec<u8>,
}


#[derive(Deserialize, Serialize, Debug)]
pub struct DataDownloadedMsg {
    pub key: DaskKey,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromWorkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
    DataDownloaded(DataDownloadedMsg),
}


#[derive(Serialize, Deserialize, Debug)]
pub struct FetchRequest {
    pub key: DaskKey,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct FetchResponseData {
    pub serializer: SerializationType,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FetchResponse {
    Data(FetchResponseData),
    NotAvailable,
}


