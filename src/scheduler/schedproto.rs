pub type WorkerId = u64;
pub type TaskId = u64;

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub ncpus: u32,
}

#[derive(Debug, Copy, Clone)]
pub enum TaskState {
    Waiting,
    Running,
    Finished,
}

#[derive(Debug)]
pub struct TaskInfo {
    pub id: TaskId,
    pub inputs: Vec<TaskId>,
}

#[derive(Debug)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskState,
    pub worker: Option<WorkerId>,
}

#[derive(Debug, Default)]
pub struct Update {
    pub task_updates: Vec<TaskUpdate>,
    pub new_tasks: Vec<TaskInfo>,
    pub new_workers: Vec<WorkerInfo>,
    pub network_bandwidth: Option<f32>,
}

#[derive(Debug)]
pub enum ToSchedulerMessage {
    Update(Update),
}

#[derive(Debug)]
pub struct SchedulerRegistration {
    pub protocol_version: u32,
    pub scheduler_name: String,
    pub scheduler_version: String,
    pub reassigning: bool,
}

#[derive(Debug)]
pub struct TaskAssignment {
    pub task: TaskId,
    pub worker: WorkerId,
    pub priority: i32,
}

#[derive(Debug)]
pub enum FromSchedulerMessage {
    TaskAssignments(Vec<TaskAssignment>),
    Register(SchedulerRegistration),
}
