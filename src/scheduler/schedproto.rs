use serde::{Deserialize, Serialize};

pub type WorkerId = u64;
pub type TaskId = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub ncpus: u32,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum TaskUpdateType {
    Running,
    // Task runs at worker
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
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Update {
    pub task_updates: Vec<TaskUpdate>,
    pub new_tasks: Vec<TaskInfo>,
    pub new_workers: Vec<WorkerInfo>,
    pub network_bandwidth: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ToSchedulerMessage {
    Update(Update),
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
