use serde::{Deserialize, Serialize};

use crate::common::data::SerializationType;
use crate::common::Map;
use crate::scheduler::{TaskId, WorkerId};
use crate::server::protocol::PriorityValue;

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerRegistrationResponse {
    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub id: TaskId,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dep_info: Vec<(TaskId, u64, Vec<WorkerId>)>,

    #[serde(with = "serde_bytes")]
    pub spec: Vec<u8>,

    pub user_priority: PriorityValue,
    pub scheduler_priority: PriorityValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskIdsMsg {
    pub ids: Vec<TaskId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerMsg {
    pub worker_id: WorkerId,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum ToWorkerMessage {
    ComputeTask(ComputeTaskMsg),
    DeleteData(TaskIdsMsg),
    StealTasks(TaskIdsMsg),
    NewWorker(NewWorkerMsg),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub id: TaskId,
    pub nbytes: u64,
    /*#[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,*/
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFailedMsg {
    pub id: TaskId,
    #[serde(with = "serde_bytes")]
    pub exception: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub traceback: Vec<u8>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DataDownloadedMsg {
    pub id: TaskId,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum StealResponse {
    Ok,
    NotHere,
    Running,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StealResponseMsg {
    pub responses: Vec<(TaskId, StealResponse)>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromWorkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
    DataDownloaded(DataDownloadedMsg),
    StealResponse(StealResponseMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchRequestMsg {
    pub task_id: TaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadDataMsg {
    pub task_id: TaskId,
    pub serializer: SerializationType,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum DataRequest {
    FetchRequest(FetchRequestMsg),
    UploadData(UploadDataMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchResponseData {
    pub serializer: SerializationType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadResponseMsg {
    pub task_id: TaskId,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum DataResponse {
    Data(FetchResponseData),
    NotAvailable,
    DataUploaded(UploadResponseMsg),
}
