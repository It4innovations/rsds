use crate::common::RcEqWrapper;
use crate::prelude::*;
use crate::scheduler::schedproto::{TaskId, TaskUpdate};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;

pub type TaskKey = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskSpec {
    #[serde(with = "serde_bytes")]
    pub function: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
}

pub enum TaskRuntimeState {
    Waiting,
    Assigned,
    Finished,
}

pub struct TaskRuntimeInfo {
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    pub consumers: HashSet<TaskRef>,
}

impl TaskRuntimeInfo {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }
}

pub struct Task {
    pub id: TaskId,
    pub key: TaskKey,
    pub info: RefCell<TaskRuntimeInfo>,
    pub spec: TaskSpec,
    pub dependencies: Vec<TaskId>,
}

pub type TaskRef = RcEqWrapper<Task>;

impl Task {
    pub fn new(id: TaskId, key: String, spec: TaskSpec, dependencies: Vec<TaskId>, unfinished_inputs: u32) -> Self {
        Task {
            id,
            key,
            spec,
            dependencies,
            info: RefCell::new(TaskRuntimeInfo {
                unfinished_inputs,
                state: TaskRuntimeState::Waiting,
                consumers: Default::default(),
            }),
        }
    }

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::TaskInfo {
        crate::scheduler::schedproto::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    pub fn make_sched_update(&self) -> Option<TaskUpdate> {
        let info = self.info.borrow();
        let state = match info.state {
            TaskRuntimeState::Finished => TaskState::Finished,
            TaskRuntimeState::Waiting | TaskRuntimeState::Assigned => {
                return None
            }
        };
        Some(TaskUpdate {
            id: self.id,
            state: state,
            worker: None,
        })
    }
}
