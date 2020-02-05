use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use crate::client::ClientId;
use crate::comm::Notifications;
use crate::common::{Set, WrappedRcRefCell};
use crate::core::Core;
use crate::protocol::clientmsg::ClientTaskSpec;
use crate::protocol::protocol::{MessageBuilder, SerializedMemory, SerializedTransport};

use crate::protocol::workermsg::{ComputeTaskMsg, ToWorkerMessage};
use crate::scheduler::schedproto::TaskId;
use crate::worker::WorkerRef;

pub type TaskKey = String;

pub enum TaskRuntimeState {
    Waiting,
    Scheduled(WorkerRef),
    Assigned(WorkerRef),
    Stealing(WorkerRef, WorkerRef), // (from, to)
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
            Self::Stealing(_, _) => 'T',
            Self::Finished(_, _) => 'F',
            Self::Released(_) => 'R',
            Self::Error(_) => 'E',
        };
        write!(f, "{}", n)
    }
}

pub struct ErrorInfo {
    pub exception: SerializedMemory,
    pub traceback: SerializedMemory,
}

#[derive(Debug)]
pub struct DataInfo {
    pub size: u64,
    pub r#type: Vec<u8>,
}

#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    consumers: Set<TaskRef>,
    pub key: TaskKey,
    pub dependencies: Vec<TaskId>,

    pub spec: ClientTaskSpec,
    subscribed_clients: Vec<ClientId>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    #[inline]
    pub fn has_consumers(&self) -> bool {
        !self.consumers.is_empty()
    }
    #[inline]
    pub fn add_consumer(&mut self, consumer: TaskRef) -> bool {
        self.consumers.insert(consumer)
    }
    #[inline]
    pub fn remove_consumer(&mut self, consumer: &TaskRef) -> bool {
        self.consumers.remove(consumer)
    }
    #[inline]
    pub fn get_consumers(&self) -> &Set<TaskRef> {
        &self.consumers
    }

    pub fn subscribe_client(&mut self, client_id: ClientId) {
        if !self.subscribed_clients.contains(&client_id) {
            self.subscribed_clients.push(client_id);
        }
    }

    pub fn unsubscribe_client(&mut self, client_id: ClientId) {
        self.subscribed_clients
            .iter()
            .position(|x| *x == client_id)
            .map(|i| self.subscribed_clients.remove(i));
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

    pub fn remove_data_if_possible(&mut self, notifications: &mut Notifications) -> bool {
        if self.consumers.is_empty() && self.subscribed_clients().is_empty() && self.is_finished() {
            // Hack for changing Finished -> Released while moving DataInfo
            let ws = match std::mem::replace(&mut self.state, TaskRuntimeState::Waiting) {
                TaskRuntimeState::Finished(data_info, ws) => {
                    self.state = TaskRuntimeState::Released(data_info);
                    ws
                }
                _ => unreachable!(),
            };

            for worker_ref in ws {
                log::debug!(
                    "Task id={} is no longer needed, deleting from worker={}",
                    self.id,
                    worker_ref.get().id
                );
                notifications.delete_key_from_worker(worker_ref, &self);
            }
            true
        } else {
            false
        }
    }

    pub fn collect_consumers(&self) -> HashSet<TaskRef> {
        let mut stack: Vec<_> = self.consumers.iter().cloned().collect();
        let mut result: HashSet<TaskRef> = stack.iter().cloned().collect();

        while let Some(task_ref) = stack.pop() {
            let task = task_ref.get();
            for t in &task.consumers {
                if result.insert(t.clone()) {
                    stack.push(t.clone());
                }
            }
        }
        result
    }

    pub fn make_compute_task_msg(
        &self,
        core: &Core,
        mbuilder: &mut MessageBuilder<ToWorkerMessage>,
    ) {
        let task_refs: Vec<_> = self
            .dependencies
            .iter()
            .map(|task_id| core.get_task_by_id_or_panic(*task_id).clone())
            .collect();
        let who_has: Vec<_> = task_refs
            .iter()
            .map(|task_ref| {
                let task = task_ref.get();
                let addresses: Vec<String> = task
                    .get_workers()
                    .unwrap()
                    .iter()
                    .map(|w| w.get().listen_address.clone())
                    .collect();
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

        let mut msg_function = SerializedTransport::Inline(rmpv::Value::Binary(vec![]));
        let mut msg_args = SerializedTransport::Inline(rmpv::Value::Binary(vec![]));
        let mut msg_kwargs = None;
        let mut msg_task = None;

        match &self.spec {
            ClientTaskSpec::Direct {
                function,
                args,
                kwargs,
            } => {
                msg_function = function.to_transport_clone(mbuilder);
                msg_args = args.to_transport_clone(mbuilder);
                msg_kwargs = kwargs.as_ref().map(|v| v.to_transport_clone(mbuilder));
            }
            ClientTaskSpec::Serialized(v) => {
                msg_task = Some(v.to_transport_clone(mbuilder));
            }
        }

        let msg = ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            key: self.key.clone(),
            duration: 0.5, // TODO
            who_has,
            nbytes,
            task: msg_task,
            function: msg_function,
            kwargs: msg_kwargs,
            args: msg_args,
        });
        mbuilder.add_message(msg);
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
    pub fn is_assigned_or_stealed_from(&self, worker_ref: &WorkerRef) -> bool {
        match &self.state {
            TaskRuntimeState::Assigned(w) | TaskRuntimeState::Stealing(w, _) => worker_ref == w,
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
    pub fn is_done(&self) -> bool {
        match &self.state {
            TaskRuntimeState::Finished(_, _)
            | TaskRuntimeState::Released(_)
            | TaskRuntimeState::Error(_) => true,
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
        spec: ClientTaskSpec,
        dependencies: Vec<TaskId>,
        unfinished_inputs: u32,
    ) -> Self {
        Self::wrap(Task {
            id,
            key,
            dependencies,
            unfinished_inputs,
            spec,
            state: TaskRuntimeState::Waiting,
            consumers: Default::default(),
            subscribed_clients: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;
    use crate::test_util::{task, task_deps};

    #[test]
    fn task_consumers_empty() {
        let a = task(0);
        assert_eq!(a.get().collect_consumers(), Default::default());
    }

    #[test]
    fn task_recursive_consumers() {
        let a = task(0);
        let b = task_deps(1, &[&a]);
        let c = task_deps(2, &[&b]);
        let d = task_deps(3, &[&b]);
        let e = task_deps(4, &[&c, &d]);

        assert_eq!(
            a.get().collect_consumers(),
            vec!(b, c, d, e).into_iter().collect()
        );
    }
}
