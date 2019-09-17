use std::collections::HashSet;

use crate::scheduler::schedproto::TaskId;

pub enum SchedulerTaskState {
    Waiting,
    Ready,
}

pub struct Task {
    pub id: TaskId,
    pub state: SchedulerTaskState,
    pub inputs: Vec<TaskRef>,
    pub consumers: HashSet<TaskRef>,
    pub unfinished_deps: u32,
}

pub type TaskRef = crate::common::WrappedRcRefCell<Task>;

impl Task {
    pub fn new(id: TaskId, inputs: Vec<TaskRef>, unfinished_deps: u32) -> Self {
        Task {
            id,
            inputs,
            state: if unfinished_deps != 0 {
                SchedulerTaskState::Waiting
            } else {
                SchedulerTaskState::Ready
            },
            unfinished_deps,
            consumers: Default::default(),
        }
    }
}
