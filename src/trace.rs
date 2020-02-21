use crate::scheduler::schedproto::TaskId;
use crate::worker::WorkerId;
macro_rules! trace_time {
    ($action:literal, $block:expr) => {
        {
            ::tracing::info!(process = "scheduler", action = $action, event = "start");
            let res = $block;
            ::tracing::info!(process = "scheduler", action = $action, event = "end");
            res
        }
    }
}

#[inline]
pub fn trace_worker_assign(task_id: TaskId, worker_id: WorkerId)
{
    tracing::info!(process = worker_id, task = task_id, action = "compute-task", event = "start");
}
#[inline]
pub fn trace_worker_finish(task_id: TaskId, worker_id: WorkerId)
{
    tracing::info!(process = worker_id, task = task_id, action = "compute-task", event = "end");
}
