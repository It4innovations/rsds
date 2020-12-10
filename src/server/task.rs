use std::collections::HashSet;
use std::fmt;

use crate::common::{Set, WrappedRcRefCell};
use crate::scheduler::protocol::TaskId;
use crate::server::core::Core;
use crate::server::notifications::Notifications;
use crate::server::protocol::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::server::protocol::PriorityValue;
use crate::server::worker::WorkerRef;

/*use crate::server::protocol::messages::worker::{
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
        };
        write!(f, "{}", n)
    }
}

#[derive(Debug)]
pub struct ErrorInfo {
    pub exception: Vec<u8>,
    pub traceback: Vec<u8>,
}

#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    consumers: Set<TaskRef>,
    pub dependencies: Vec<TaskId>,

    //pub spec: Option<ClientTaskSpec>,
    pub spec: Vec<u8>, // Serialized TaskSpec

    pub user_priority: i32,
    pub scheduler_priority: i32,
    pub client_priority: i32,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    #[inline]
    pub fn id(&self) -> TaskId {
        self.id
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

    /*pub fn subscribe_client(&mut self, client_id: ClientId) {
        if !self.subscribed_clients.contains(&client_id) {
            self.subscribed_clients.push(client_id);
        }
    }*/

    pub fn make_sched_info(&self) -> crate::scheduler::protocol::TaskInfo {
        crate::scheduler::protocol::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    pub fn remove_data_if_possible(&mut self, core: &mut Core, notifications: &mut Notifications) {
        if self.consumers.is_empty() && self.is_finished() && !core.gateway.is_kept(self.id) {
            let ws = match core.remove_task(self) {
                TaskRuntimeState::Finished(_, ws) => ws,
                _ => unreachable!(),
            };
            for worker_ref in ws {
                log::debug!(
                    "Task id={} is no longer needed, deleting from worker={}",
                    self.id,
                    worker_ref.get().id
                );
                notifications.delete_data_from_worker(worker_ref, &self);
            }
            notifications.remove_task(&self);
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
                    .map(|w| w.get().id())
                    .collect();
                (task.id, task.data_info().unwrap().size, addresses)
            })
            .collect();

        /*let unpack = |s: &SerializedMemory| match s {
            SerializedMemory::Inline(v) => v.clone(),
            SerializedMemory::Indexed { frames, header: _ } => {
                assert_eq!(frames.len(), 1);
                frames[0].to_vec().into()
            }
        };*/

        /*let (function, args, kwargs) = match &self.spec {
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
        };*/

        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: self.id,
            dep_info,
            spec: self.spec.clone(),
            user_priority: self.user_priority,
            scheduler_priority: self.scheduler_priority,
        })
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
            TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Released => true,
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
        spec: Vec<u8>,
        dependencies: Vec<TaskId>,
        unfinished_inputs: u32,
        user_priority: PriorityValue,
        client_priority: PriorityValue,
    ) -> Self {
        Self::wrap(Task {
            id,
            dependencies,
            unfinished_inputs,
            spec,
            user_priority,
            client_priority,
            scheduler_priority: Default::default(),
            state: TaskRuntimeState::Waiting,
            consumers: Default::default(),
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
