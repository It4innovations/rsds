use crate::common::RcEqWrapper;

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;

type TaskKey = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskSpec {
    #[serde(with = "serde_bytes")]
    pub function: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
}

pub enum TaskState {
    Waiting,
    Ready,
    Assigned,
    Finished,
}

pub struct TaskRuntimeInfo {
    pub state: TaskState,
    pub unfinished_deps: u32,
    pub consumers: HashSet<TaskRef>,
}

pub struct Task {
    pub key: TaskKey,
    pub info: RefCell<TaskRuntimeInfo>,
    pub spec: TaskSpec,
    pub dependencies: Vec<TaskKey>,
}

pub type TaskRef = RcEqWrapper<Task>;

impl Task {
    pub fn new(
        key: String,
        spec: TaskSpec,
        dependencies: Vec<TaskKey>,
        unfinished_deps: u32,
    ) -> Self {
        Task {
            key,
            spec,
            dependencies,
            info: RefCell::new(TaskRuntimeInfo {
                state: if unfinished_deps != 0 {
                    TaskState::Waiting
                } else {
                    TaskState::Ready
                },
                unfinished_deps,
                consumers: Default::default(),
            }),
        }
    }
}
