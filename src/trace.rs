use crate::scheduler::schedproto::TaskId;
use crate::worker::WorkerId;

pub struct TimedScope<'a> {
    process: &'a str,
    method: &'static str
}

impl<'a> TimedScope<'a> {
    pub fn new(process: &'a str, method: &'static str) -> Self {
        tracing::info!(action = "measure", process = process, method = method, event = "start");
        Self { process, method }
    }
}

impl <'a> Drop for TimedScope<'a> {
    fn drop(&mut self) {
        tracing::info!(action = "measure", method = self.method, process = self.process, event = "end");
    }
}

macro_rules! trace_time {
    ($process:tt, $method:tt, $block:expr) => {
        {
            let _ = $crate::trace::TimedScope::new($process, $method);
            let res = $block;
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
pub fn trace_task_finish(task_id: TaskId, task_key: &str, worker_id: WorkerId, duration: u64)
{
    tracing::info!(action = "compute-task", event = "end", task = task_id, task_key = task_key, worker = worker_id, duration = duration);
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
pub fn trace_worker_steal_response(task_id: TaskId, from: WorkerId, to: WorkerId, result: &str)
{
    tracing::info!(action = "steal-response", task = task_id, from = from, to = to, result = result);
}
#[inline]
pub fn trace_worker_steal_response_missing(task_key: &str, from: WorkerId)
{
    tracing::info!(action = "steal-response", task = task_key, from = from, to = 0, result = "missing");
}
#[inline]
pub fn trace_packet_send(size: usize)
{
    tracing::info!(action = "packet-send", size = size);
}
#[inline]
pub fn trace_packet_receive(size: usize)
{
    tracing::info!(action = "packet-receive", size = size);
}
