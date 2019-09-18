use std::collections::HashSet;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use crate::common::WrappedRcRefCell;
use crate::core::Core;
use crate::messages::workermsg::ComputeTaskMsg;
use crate::prelude::*;
use crate::scheduler::schedproto::TaskId;
use crate::messages::aframe::AdditionalFrame;

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
    Released,
    Error,
}

pub struct ErrorInfo {
    pub frames: Vec<AdditionalFrame>,
}

pub struct ResultInfo {
    pub size: u64,
    pub r#type: Vec<u8>,
}

pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    pub consumers: HashSet<TaskRef>,
    pub worker: Option<WorkerRef>,
    pub result_info: Option<ResultInfo>,

    pub error: Option<Rc<ErrorInfo>>,

    pub key: TaskKey,
    pub spec: TaskSpec,
    pub dependencies: Vec<TaskId>,

    pub subscribed_clients: HashSet<ClientId>,
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

    pub fn make_compute_task_msg(&self, core: &Core) -> ComputeTaskMsg {
        let task_refs: Vec<_> = self
            .dependencies
            .iter()
            .map(|task_id| core.get_task_by_id_or_panic(*task_id).clone())
            .collect();
        let who_has: Vec<_> = task_refs
            .iter()
            .map(|task_ref| {
                let task = task_ref.get();
                let worker = task.worker.as_ref().unwrap().get();
                (task.key.clone(), vec![worker.listen_address.clone()])
            })
            .collect();

        let nbytes: Vec<_> = task_refs
            .iter()
            .map(|task_ref| {
                let task = task_ref.get();
                (task.key.clone(), task.result_info.as_ref().unwrap().size)
            })
            .collect();

        ComputeTaskMsg {
            key: self.key.clone(),
            function: self.spec.function.clone(),
            args: self.spec.args.clone(),
            duration: 0.5, // TODO
            who_has,
            nbytes,
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
            result_info: None,
            error: None,
            subscribed_clients: Default::default(),
        })
    }
}
