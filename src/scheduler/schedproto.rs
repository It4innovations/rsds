use serde::{Deserialize, Serialize};

pub type WorkerId = u64;
pub type TaskId = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub n_cpus: u32,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum TaskUpdateType {
    Finished,
    // Task was computed on a worker
    Placed,
    // Task data are available on worker
    Removed,
    // Task data are no available on worker (or running state is cancelled)
    Discard, // Task is removed from system, do not schedule it
    // & it is no longer available on on any worker
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: TaskId,
    pub inputs: Vec<TaskId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskUpdateType,
    pub worker: WorkerId,
    pub size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ToSchedulerMessage {
    TaskUpdate(TaskUpdate),
    NewTask(TaskInfo),
    NewWorker(WorkerInfo),
    NetworkBandwidth(f32),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerRegistration {
    pub protocol_version: u32,
    pub scheduler_name: String,
    pub scheduler_version: String,
    pub reassigning: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: TaskId,
    pub worker: WorkerId,
    pub priority: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FromSchedulerMessage {
    TaskAssignments(Vec<TaskAssignment>),
    Register(SchedulerRegistration),
}
