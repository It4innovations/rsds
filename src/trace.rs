use crate::scheduler::schedproto::TaskId;
use crate::worker::WorkerId;
macro_rules! trace_time {
    ($action:literal, $block:expr) => {
        {
            ::tracing::info!(action = $action, event = "start");
            let res = $block;
            ::tracing::info!(action = $action, event = "end");
            res
        }
    }
}

#[inline]
pub fn trace_worker_assign(task_id: TaskId, worker_id: WorkerId)
{
    tracing::info!(action = "compute-task", event = "start", worker = worker_id, task = task_id);
}
#[inline]
pub fn trace_worker_finish(task_id: TaskId, worker_id: WorkerId)
{
    tracing::info!(action = "compute-task", event = "end", task = task_id, worker = worker_id,);
}
#[inline]
pub fn trace_new_worker(worker_id: WorkerId, ncpus: u32)
{
    tracing::info!(action = "new-worker", worker_id = worker_id, cpus = ncpus);
}
#[inline]
pub fn trace_worker_steal(task_id: TaskId, from: WorkerId, to: WorkerId)
{
    tracing::info!(action = "steal", task = task_id, from = from, to = to);
}
#[inline]
pub fn trace_worker_steal_response(task_id: TaskId, from: WorkerId, to: WorkerId, success: bool)
{
    tracing::info!(action = "steal-response", task = task_id, from = from, to = to, success = success);
}
