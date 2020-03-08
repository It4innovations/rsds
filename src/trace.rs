use crate::scheduler::TaskId;
use crate::worker::WorkerId;
use std::fmt::Write;

pub struct ScopedTimer<'a> {
    process: &'a str,
    method: &'static str,
}

impl<'a> ScopedTimer<'a> {
    pub fn new(process: &'a str, method: &'static str) -> Self {
        tracing::info!(
            action = "measure",
            process = process,
            method = method,
            event = "start"
        );
        Self { process, method }
    }
}

impl<'a> Drop for ScopedTimer<'a> {
    fn drop(&mut self) {
        tracing::info!(
            action = "measure",
            method = self.method,
            process = self.process,
            event = "end"
        );
    }
}

macro_rules! trace_time {
    ($process:tt, $method:tt, $block:expr) => {{
        let _timer = $crate::trace::ScopedTimer::new($process, $method);
        $block
    }};
}

#[inline(always)]
pub fn trace_task_new(task_id: TaskId, key: &str, inputs: &[u64]) {
    let make_inputs = || {
        let mut input_str = String::with_capacity(2 * inputs.len());
        for input in inputs {
            write!(input_str, "{},", input).ok();
        }
        input_str
    };

    tracing::info!(
        action = "task",
        event = "create",
        task = task_id,
        key = key,
        inputs = make_inputs().as_str()
    );
}
#[inline(always)]
pub fn trace_task_new_finished(task_id: TaskId, key: &str, size: u64, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "create",
        task = task_id,
        key = key,
        worker = worker_id,
        size = size
    );
}
#[inline(always)]
pub fn trace_task_assign(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "assign",
        worker = worker_id,
        task = task_id
    );
}
#[inline(always)]
pub fn trace_task_send(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "send",
        task = task_id,
        worker = worker_id,
    );
}
#[inline(always)]
pub fn trace_task_place(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "place",
        task = task_id,
        worker = worker_id,
    );
}
#[inline(always)]
pub fn trace_task_finish(task_id: TaskId, worker_id: WorkerId, size: u64, duration: (u64, u64)) {
    tracing::info!(
        action = "task",
        event = "finish",
        task = task_id,
        worker = worker_id,
        start = duration.0,
        stop = duration.1,
        size = size
    );
}
#[inline(always)]
pub fn trace_task_remove(task_id: TaskId) {
    tracing::info!(action = "task", event = "remove", task = task_id,);
}
#[inline(always)]
pub fn trace_worker_new(worker_id: WorkerId, ncpus: u32, address: &str) {
    tracing::info!(
        action = "new-worker",
        worker_id = worker_id,
        cpus = ncpus,
        address = address
    );
}
#[inline(always)]
pub fn trace_worker_steal(task_id: TaskId, from: WorkerId, to: WorkerId) {
    tracing::info!(action = "steal", task = task_id, from = from, to = to);
}
#[inline(always)]
pub fn trace_worker_steal_response(task_id: TaskId, from: WorkerId, to: WorkerId, result: &str) {
    tracing::info!(
        action = "steal-response",
        task = task_id,
        from = from,
        to = to,
        result = result
    );
}
#[inline(always)]
pub fn trace_worker_steal_response_missing(task_key: &str, from: WorkerId) {
    tracing::info!(
        action = "steal-response",
        task = task_key,
        from = from,
        to = 0,
        result = "missing"
    );
}
#[inline(always)]
pub fn trace_packet_send(size: usize) {
    tracing::info!(action = "packet-send", size = size);
}
#[inline(always)]
pub fn trace_packet_receive(size: usize) {
    tracing::info!(action = "packet-receive", size = size);
}
