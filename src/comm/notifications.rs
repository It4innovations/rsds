use crate::client::ClientId;
use crate::common::Map;
use crate::protocol::key::DaskKey;
use crate::scheduler::protocol::{
    NewFinishedTaskInfo, TaskStealResponse, TaskUpdate, TaskUpdateType,
};
use crate::scheduler::ToSchedulerMessage;
use crate::task::{Task, TaskRef};
use crate::worker::{Worker, WorkerRef};

#[derive(Default, Debug)]
pub(crate) struct WorkerNotification {
    pub(crate) compute_tasks: Vec<TaskRef>,
    pub(crate) delete_keys: Vec<DaskKey>,
    pub(crate) steal_tasks: Vec<TaskRef>,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Default, Debug)]
pub(crate) struct ClientNotification {
    pub(crate) in_memory_tasks: Vec<TaskRef>,
    pub(crate) error_tasks: Vec<TaskRef>,
}

#[derive(Default, Debug)]
pub struct Notifications {
    pub(crate) workers: Map<WorkerRef, WorkerNotification>,
    pub(crate) clients: Map<ClientId, ClientNotification>,
    pub(crate) scheduler_messages: Vec<ToSchedulerMessage>,
}

impl Notifications {
    pub fn with_scheduler_capacity(capacity: usize) -> Self {
        Self {
            workers: Default::default(),
            clients: Default::default(),
            scheduler_messages: Vec::with_capacity(capacity),
        }
    }

    pub fn new_worker(&mut self, worker: &Worker) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewWorker(worker.make_sched_info()));
    }

    #[inline]
    pub fn new_task(&mut self, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewTask(task.make_sched_info()));
    }

    #[inline]
    pub fn new_finished_task(&mut self, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewFinishedTask(NewFinishedTaskInfo {
                id: task.id,
                workers: task
                    .get_workers()
                    .unwrap()
                    .iter()
                    .map(|wr| wr.get().id)
                    .collect(),
                size: task.data_info().unwrap().size,
            }));
    }

    pub fn remove_task(&mut self, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::RemoveTask(task.id));
    }

    pub fn delete_key_from_worker(&mut self, worker_ref: WorkerRef, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Removed,
                id: task.id,
                worker: worker_ref.get().id,
                size: None,
            }));
        self.workers
            .entry(worker_ref)
            .or_default()
            .delete_keys
            .push(task.key().into());
    }

    pub fn task_placed(&mut self, worker: &Worker, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Placed,
                id: task.id,
                worker: worker.id,
                size: None,
            }));
    }

    pub fn task_finished(&mut self, worker: &Worker, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Finished,
                id: task.id,
                worker: worker.id,
                size: Some(task.data_info().unwrap().size),
            }));
    }

    pub fn task_steal_response(
        &mut self,
        from_worker: &Worker,
        to_worker: &Worker,
        task: &Task,
        success: bool,
    ) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskStealResponse(TaskStealResponse {
                id: task.id,
                from_worker: from_worker.id,
                to_worker: to_worker.id,
                success,
            }));
    }

    pub fn compute_task_on_worker(&mut self, worker_ref: WorkerRef, task_ref: TaskRef) {
        self.workers
            .entry(worker_ref)
            .or_default()
            .compute_tasks
            .push(task_ref);
    }

    pub fn steal_task_from_worker(&mut self, worker_ref: WorkerRef, task_ref: TaskRef) {
        self.workers
            .entry(worker_ref)
            .or_default()
            .steal_tasks
            .push(task_ref);
    }

    pub fn notify_client_about_task_error(&mut self, client_id: ClientId, task_ref: TaskRef) {
        self.clients
            .entry(client_id)
            .or_default()
            .error_tasks
            .push(task_ref);
    }

    pub fn notify_client_key_in_memory(&mut self, client_id: ClientId, task_ref: TaskRef) {
        self.clients
            .entry(client_id)
            .or_default()
            .in_memory_tasks
            .push(task_ref);
    }
}
