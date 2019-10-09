use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use crate::common::WrappedRcRefCell;
use crate::core::Core;
use crate::messages::aframe::{AdditionalFrame, AfHeader, MessageBuilder};
use crate::messages::clientmsg::TaskSpec;
use crate::messages::workermsg::ComputeTaskMsg;
use crate::messages::workermsg::ToWorkerMessage;
use crate::notifications::Notifications;
use crate::prelude::*;
use crate::scheduler::schedproto::TaskId;

pub type TaskKey = String;

pub enum TaskRuntimeState {
    Waiting,
    Scheduled(WorkerRef),
    Assigned(WorkerRef),
    Finished(DataInfo, Vec<WorkerRef>),
    Released(DataInfo),
    Error(Rc<ErrorInfo>),
}

impl fmt::Debug for TaskRuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = match self {
            Self::Waiting => 'W',
            Self::Scheduled(_) => 'S',
            Self::Assigned(_) => 'A',
            Self::Finished(_, _) => 'F',
            Self::Released(_) => 'R',
            Self::Error(_) => 'E',
        };
        write!(f, "{}", n)
    }
}

pub struct ErrorInfo {
    pub frames: Vec<AdditionalFrame>,
}

#[derive(Debug)]
pub struct DataInfo {
    pub size: u64,
    pub r#type: Vec<u8>,
}


pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    pub consumers: HashSet<TaskRef>,
    //pub worker: Option<WorkerRef>,
    pub key: TaskKey,
    pub dependencies: Vec<TaskId>,

    pub function_data: Vec<u8>,
    pub args_data: Vec<u8>,
    pub args_header: Option<AfHeader>,

    subscribed_clients: Vec<ClientId>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    pub fn subscribe_client(&mut self, client_id: ClientId) {
        if !self.subscribed_clients.contains(&client_id) {
            self.subscribed_clients.push(client_id);
        }
    }

    pub fn unsubscribe_client(&mut self, client_id: ClientId) {
        self.subscribed_clients.iter().position(|x| *x == client_id).map(|i| self.subscribed_clients.remove(i));
    }

    pub fn subscribed_clients(&self) -> &Vec<ClientId> {
        &self.subscribed_clients
    }

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::TaskInfo {
        crate::scheduler::schedproto::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    pub fn check_if_data_cannot_be_removed(&mut self, notifications: &mut Notifications) {
        if self.consumers.is_empty() && self.subscribed_clients().is_empty() && self.is_finished() {

            // Hack for changing Finished -> Released while moving DataInfo
            let ws = match std::mem::replace(&mut self.state, TaskRuntimeState::Waiting) {
                TaskRuntimeState::Finished(data_info, ws) => {
                    self.state = TaskRuntimeState::Released(data_info);
                    ws
                }
                _ => unreachable!()
            };

            for worker_ref in ws {
                log::debug!("Task id={} is no longer needed, deleting from worker={}", self.id, worker_ref.get().id);
                notifications.delete_key_from_worker(worker_ref, &self);
            }
        }
    }

    pub fn collect_consumers(&self) -> HashSet<TaskRef>
    {
        let mut stack: Vec<_> = self.consumers.iter().cloned().collect();
        let mut result: HashSet<TaskRef> = stack.iter().cloned().collect();

        while !stack.is_empty() {
            let task_ref = stack.pop().unwrap();
            let task = task_ref.get();
            for t in &task.consumers {
                if !result.contains(&t) {
                    result.insert(t.clone());
                    stack.push(t.clone());
                }
            }
        }
        result
    }

    pub fn make_compute_task_msg(&self, core: &Core, mbuilder: &mut MessageBuilder<ToWorkerMessage>) {
        let task_refs: Vec<_> = self
            .dependencies
            .iter()
            .map(|task_id| core.get_task_by_id_or_panic(*task_id).clone())
            .collect();
        let who_has: Vec<_> = task_refs
            .iter()
            .map(|task_ref| {
                let task = task_ref.get();
                let addresses = task.get_workers().unwrap().iter().map(|w| w.get().listen_address.clone()).collect();
                (task.key.clone(), addresses)
            })
            .collect();

        let nbytes: Vec<_> = task_refs
            .iter()
            .map(|task_ref| {
                let task = task_ref.get();
                (task.key.clone(), task.data_info().unwrap().size)
            })
            .collect();

        mbuilder.add_message(ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            key: self.key.clone(),
            function: self.function_data.clone(),
            args: self.args_data.clone(),
            duration: 0.5, // TODO
            who_has,
            nbytes,
        }));
    }

    #[inline]
    pub fn is_waiting(&self) -> bool {
        match &self.state {
            TaskRuntimeState::Waiting => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_scheduled(&self) -> bool {
        match &self.state {
            TaskRuntimeState::Scheduled(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_assigned(&self) -> bool {
        match &self.state {
            TaskRuntimeState::Assigned(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_assigned_on(&self, worker_ref: &WorkerRef) -> bool {
        match &self.state {
            TaskRuntimeState::Assigned(w) => worker_ref == w,
            _ => false,
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match &self.state {
            TaskRuntimeState::Finished(_, _) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn data_info(&self) -> Option<&DataInfo> {
        match &self.state {
            TaskRuntimeState::Finished(data, _) => Some(data),
            TaskRuntimeState::Released(data) => Some(data),
            _ => None,
        }
    }

    #[inline]
    pub fn get_workers(&self) -> Option<&Vec<WorkerRef>> {
        match &self.state {
            TaskRuntimeState::Finished(_, ws) => Some(ws),
            _ => None,
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
            dependencies,
            unfinished_inputs,
            function_data: spec.function,
            args_data: spec.args,
            args_header: None,
            state: TaskRuntimeState::Waiting,
            consumers: Default::default(),
            subscribed_clients: Default::default(),
        })
    }
}