use crate::common::{RcEqWrapper, WrappedRcRefCell};
use crate::messages::workermsg::ComputeTaskMsg;
use crate::prelude::*;
use crate::scheduler::schedproto::{TaskId, TaskUpdate};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;
use crate::core::Core;

pub type TaskKey = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskSpec {
    #[serde(with = "serde_bytes")]
    pub function: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
}

#[derive(PartialEq, Debug)]
pub enum TaskRuntimeState {
    Waiting,
    Scheduled,
    Assigned,
    Finished,
}

pub struct TaskRuntimeInfo {}

impl TaskRuntimeInfo {}

pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    pub consumers: HashSet<TaskRef>,
    pub worker: Option<WorkerRef>,
    pub size: Option<u64>,

    pub key: TaskKey,
    pub spec: TaskSpec,
    pub dependencies: Vec<TaskId>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::TaskInfo {
        crate::scheduler::schedproto::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    pub fn make_sched_update(&self) -> Option<TaskUpdate> {
        let state = match self.state {
            TaskRuntimeState::Finished => TaskState::Finished,
            TaskRuntimeState::Waiting
            | TaskRuntimeState::Scheduled
            | TaskRuntimeState::Assigned => return None,
        };
        Some(TaskUpdate {
            id: self.id,
            state,
            worker: None,
        })
    }

    pub fn make_compute_task_msg(&self, core: &Core) -> ComputeTaskMsg {
        let task_refs : Vec<_> = self.dependencies.iter().map(|task_id| { core.get_task_by_id_or_panic(*task_id).clone() }).collect();
        let who_has : Vec<_> = task_refs.iter().map(|task_ref| {
            let task = task_ref.get();
            let worker = task.worker.as_ref().unwrap().get();
            (task.key.clone(), vec![worker.listen_address.clone()])
        }).collect();

        let nbytes : Vec<_> = task_refs.iter().map(|task_ref| {
            let task = task_ref.get();
            (task.key.clone(), task.size.unwrap())
        }).collect();


        ComputeTaskMsg {
            key: self.key.clone(),
            function: self.spec.function.clone(),
            args: self.spec.args.clone(),
            duration: 0.5, //
            who_has, nbytes
        }
    }
}

impl TaskRef {
    pub fn new(
        id: TaskId,
        key: String,
        spec: TaskSpec,
        dependencies: Vec<TaskId>,
        unfinished_inputs: u32,
    ) -> Self {
        WrappedRcRefCell::wrap(Task {
            id,
            key,
            spec,
            dependencies,
            unfinished_inputs,
            state: TaskRuntimeState::Waiting,
            consumers: Default::default(),
            worker: None,
            size: None,
        })
    }
}
