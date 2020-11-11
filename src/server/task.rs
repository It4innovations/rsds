use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use crate::common::{Set, WrappedRcRefCell};
use crate::scheduler::protocol::TaskId;
use crate::server::client::ClientId;
use crate::server::core::Core;
use crate::server::notifications::Notifications;
use crate::server::protocol::daskmessages::client::{ClientTaskSpec, DirectTaskSpec};
use crate::server::protocol::key::{DaskKey, DaskKeyRef};
use crate::server::protocol::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::server::protocol::PriorityValue;
use crate::server::worker::WorkerRef;

/*use crate::server::protocol::daskmessages::worker::{
    ComputeTaskMsg as DaskComputeTaskMsg, ToWorkerMessage as DaskToWorkerMessage,
};*/
#[derive(Debug)]
pub struct DataInfo {
    pub size: u64,
    //pub r#type: Vec<u8>,
}

pub enum TaskRuntimeState {
    Waiting,
    Scheduled(WorkerRef),
    Assigned(WorkerRef),
    Stealing(WorkerRef, WorkerRef), // (from, to)
    Finished(DataInfo, Set<WorkerRef>),
    Released,
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
            Self::Released => 'R',
            Self::Error(_) => 'E',
        };
        write!(f, "{}", n)
    }
}

pub struct ErrorInfo {
    pub exception: Vec<u8>,
    pub traceback: Vec<u8>,
}

#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    pub actor: bool,
    consumers: Set<TaskRef>,
    key: DaskKey,
    pub dependencies: Vec<TaskId>,

    pub spec: Option<ClientTaskSpec>,

    pub user_priority: i32,
    pub scheduler_priority: i32,
    pub client_priority: i32,
    subscribed_clients: Vec<ClientId>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    #[inline]
    pub fn key_ref(&self) -> &DaskKeyRef {
        &self.key
    }

    #[inline]
    pub fn key(&self) -> &DaskKey {
        &self.key
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

    #[inline]
    pub fn has_subscribed_clients(&self) -> bool {
        !self.consumers.is_empty()
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

    pub fn make_sched_info(&self) -> crate::scheduler::protocol::TaskInfo {
        crate::scheduler::protocol::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    pub fn remove_data_if_possible(&mut self, core: &mut Core, notifications: &mut Notifications) {
        if self.consumers.is_empty() && self.subscribed_clients().is_empty() && self.is_finished() {
            let ws = match std::mem::replace(&mut self.state, TaskRuntimeState::Released) {
                TaskRuntimeState::Finished(_, ws) => ws,
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
            notifications.remove_task(&self);
            core.remove_task(&self);
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

    pub fn make_compute_task_msg_rsds(&self, core: &Core) -> ToWorkerMessage {
        let dep_info: Vec<_> = self
            .dependencies
            .iter()
            .map(|task_id| {
                let task_ref = core.get_task_by_id_or_panic(*task_id);
                let task = task_ref.get();
                let addresses: Vec<_> = task
                    .get_workers()
                    .unwrap()
                    .iter()
                    .map(|w| w.get().listen_address.clone())
                    .collect();
                (task.key.clone(), task.data_info().unwrap().size, addresses)
            })
            .collect();

        /*let unpack = |s: &SerializedMemory| match s {
            SerializedMemory::Inline(v) => v.clone(),
            SerializedMemory::Indexed { frames, header: _ } => {
                assert_eq!(frames.len(), 1);
                frames[0].to_vec().into()
            }
        };*/

        let (function, args, kwargs) = match &self.spec {
            Some(ClientTaskSpec::Direct(DirectTaskSpec {
                function,
                args,
                kwargs,
            })) => (
                function.as_ref().unwrap().to_msgpack_value(),
                args.as_ref().unwrap().to_msgpack_value(),
                kwargs.as_ref().map(|x| x.to_msgpack_value()),
            ),
            Some(ClientTaskSpec::Serialized(s)) => (s.to_msgpack_value(), rmpv::Value::Nil, None),
            None => panic!("Task has no specification"),
        };

        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            key: self.key.clone(),
            dep_info,
            function,
            args,
            kwargs,
            user_priority: self.user_priority,
            scheduler_priority: self.scheduler_priority,
        })
    }

    /*pub fn make_compute_task_msg_dask(
        &self,
        core: &Core,
        mbuilder: &mut MessageBuilder<DaskToWorkerMessage>,
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
                let addresses: Vec<_> = task
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

        let mut msg_function = None;
        let mut msg_args = None;
        let mut msg_kwargs = None;
        let mut msg_task = None;

        match &self.spec {
            Some(ClientTaskSpec::Direct(DirectTaskSpec {
                function,
                args,
                kwargs,
            })) => {
                msg_function = function.as_ref().map(|v| v.to_transport_clone(mbuilder));
                msg_args = args.as_ref().map(|v| v.to_transport_clone(mbuilder));
                msg_kwargs = kwargs.as_ref().map(|v| v.to_transport_clone(mbuilder));
            }
            Some(ClientTaskSpec::Serialized(v)) => {
                msg_task = Some(v.to_transport_clone(mbuilder));
            }
            None => panic!("Task has no specification"),
        };

        let msg = DaskToWorkerMessage::ComputeTask(DaskComputeTaskMsg {
            key: self.key.clone(),
            duration: 0.5, // TODO
            actor: self.actor,
            who_has,
            nbytes,
            task: msg_task,
            function: msg_function,
            kwargs: msg_kwargs,
            args: msg_args,
            priority: [
                self.user_priority,
                self.scheduler_priority,
                self.client_priority,
            ],
        });
        mbuilder.add_message(msg);
    }*/

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
            | TaskRuntimeState::Released
            | TaskRuntimeState::Error(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn data_info(&self) -> Option<&DataInfo> {
        match &self.state {
            TaskRuntimeState::Finished(data, _) => Some(data),
            _ => None,
        }
    }

    #[inline]
    pub fn get_workers(&self) -> Option<&Set<WorkerRef>> {
        match &self.state {
            TaskRuntimeState::Finished(_, ws) => Some(ws),
            _ => None,
        }
    }
}

impl TaskRef {
    pub fn new(
        id: TaskId,
        key: DaskKey,
        spec: Option<ClientTaskSpec>,
        dependencies: Vec<TaskId>,
        unfinished_inputs: u32,
        user_priority: PriorityValue,
        client_priority: PriorityValue,
    ) -> Self {
        Self::wrap(Task {
            id,
            key,
            actor: false,
            dependencies,
            unfinished_inputs,
            spec,
            user_priority,
            client_priority,
            scheduler_priority: Default::default(),
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
