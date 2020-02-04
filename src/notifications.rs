use crate::client::ClientId;
use crate::core::Core;

use crate::scheduler::schedproto::{TaskStealResponse, TaskUpdate, TaskUpdateType};
use crate::scheduler::ToSchedulerMessage;

use crate::common::Map;
use crate::protocol::clientmsg::{TaskErredMsg, ToClientMessage};
use crate::protocol::protocol::MessageBuilder;
use crate::protocol::workermsg::{DeleteDataMsg, StealRequestMsg, ToWorkerMessage};
use crate::task::{Task, TaskKey, TaskRef, TaskRuntimeState};
use crate::worker::{Worker, WorkerRef};

#[derive(Default)]
pub struct WorkerNotification {
    pub compute_tasks: Vec<TaskRef>,
    pub delete_keys: Vec<TaskKey>,
    pub steal_tasks: Vec<TaskRef>,
}

#[derive(Default)]
pub struct ClientNotification {
    pub error_tasks: Vec<TaskRef>,
}

#[derive(Default)]
pub struct Notifications {
    workers: Map<WorkerRef, WorkerNotification>,
    clients: Map<ClientId, ClientNotification>,
    scheduler_messages: Vec<ToSchedulerMessage>,
}

impl Notifications {
    pub fn new_worker(&mut self, worker: &Worker) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewWorker(worker.make_sched_info()));
    }

    #[inline]
    pub fn new_task(&mut self, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewTask(task.make_sched_info()));
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
            .push(task.key.clone());
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

    pub fn send(self, core: &mut Core) -> crate::Result<()> {
        /* Send to scheduler */
        if !self.scheduler_messages.is_empty() {
            core.send_scheduler_messages(self.scheduler_messages);
        }

        /* Send to workers */
        for (worker_ref, w_update) in self.workers {
            let mut mbuilder = MessageBuilder::new();

            for task in w_update.compute_tasks {
                task.get().make_compute_task_msg(core, &mut mbuilder);
            }

            if !w_update.delete_keys.is_empty() {
                mbuilder.add_message(ToWorkerMessage::DeleteData(DeleteDataMsg {
                    keys: w_update.delete_keys,
                    report: false,
                }));
            }

            for tref in w_update.steal_tasks {
                let task = tref.get();
                mbuilder.add_message(ToWorkerMessage::StealRequest(StealRequestMsg {
                    key: task.key.clone(),
                }));
            }

            if !mbuilder.is_empty() {
                let mut worker = worker_ref.get_mut();
                worker
                    .send_dask_message(
                        mbuilder
                            .build_batch()
                            .expect("Could not build worker notification messages"),
                    )
                    .unwrap_or_else(|_| {
                        // !!! Do not propagate error right now, we need to finish sending protocol to others
                        // Worker cleanup is done elsewhere (when worker future terminates),
                        // so we can safely ignore this. Since we are nice guys we log (debug) message.
                        log::debug!("Sending tasks to worker {} failed", worker.id);
                    });
            }
        }

        /* Send to clients */
        for (client_id, c_update) in self.clients {
            if !c_update.error_tasks.is_empty() {
                let mut mbuilder =
                    MessageBuilder::<ToClientMessage>::with_capacity(c_update.error_tasks.len());

                for task_ref in c_update.error_tasks.iter() {
                    let task = task_ref.get();
                    if let TaskRuntimeState::Error(error_info) = &task.state {
                        let exception = mbuilder.copy_serialized(&error_info.exception);
                        let traceback = mbuilder.copy_serialized(&error_info.traceback);
                        mbuilder.add_message(ToClientMessage::TaskErred(TaskErredMsg {
                            key: task.key.clone(),
                            exception,
                            traceback,
                        }));
                    } else {
                        panic!("Task is not in error state");
                    };
                }

                let client = core.get_client_by_id_or_panic(client_id);
                client.send_dask_message(mbuilder.build_batch()?).unwrap();
            }
        }
        Ok(())
    }
}
