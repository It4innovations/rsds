use futures::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::common::{Map, WrappedRcRefCell};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::server::core::{Core, CoreRef};
use crate::server::notifications::Notifications;
use crate::server::notifications::WorkerNotification;
use crate::server::protocol::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::server::worker::WorkerRef;
use crate::trace::trace_task_send;
use crate::Error;

pub type CommRef = WrappedRcRefCell<Comm>;

pub struct Comm {
    sender: UnboundedSender<Vec<ToSchedulerMessage>>,
}

impl Comm {
    pub fn notify(&mut self, core: &mut Core, notifications: Notifications) -> crate::Result<()> {
        if !notifications.scheduler_messages.is_empty() {
            self.sender.send(notifications.scheduler_messages).unwrap();
        }
        self.notify_workers(&core, notifications.workers)?;

        if !notifications.client_notifications.is_empty() {
            core.gateway
                .send_notifications(notifications.client_notifications);
        }
        Ok(())
    }

    fn notify_workers(
        &mut self,
        core: &Core,
        notifications: Map<WorkerRef, WorkerNotification>,
    ) -> crate::Result<()> {
        for (worker_ref, w_update) in notifications {
            let worker = worker_ref.get();
            for task in w_update.compute_tasks {
                let task = task.get();
                trace_task_send(task.id, worker_ref.get().id);
                let msg = task.make_compute_task_msg_rsds(core);
                worker.send_message(msg);
            }
            if !w_update.delete_keys.is_empty() {
                let message = ToWorkerMessage::DeleteData(TaskIdsMsg {
                    ids: w_update.delete_keys,
                });
                worker.send_message(message);
            }

            if !w_update.steal_tasks.is_empty() {
                let ids: Vec<_> = w_update.steal_tasks.iter().map(|t| t.get().id()).collect();
                let message = ToWorkerMessage::StealTasks(TaskIdsMsg { ids });
                worker.send_message(message);
            }
        }
        Ok(())
    }
}

impl CommRef {
    pub fn new(sender: UnboundedSender<Vec<ToSchedulerMessage>>) -> Self {
        Self::wrap(Comm { sender })
    }
}

pub async fn observe_scheduler(
    core_ref: CoreRef,
    comm_ref: CommRef,
    mut receiver: UnboundedReceiver<FromSchedulerMessage>,
) -> crate::Result<()> {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
            log::debug!("Scheduler registered: {:?}", r)
        }
        None => {
            return Err(Error::SchedulerError(
                "Scheduler closed connection without registration".to_owned(),
            ));
        }
        _ => {
            return Err(Error::SchedulerError(
                "First message of scheduler has to be registration".to_owned(),
            ));
        }
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut notifications = Default::default();

                trace_time!("core", "process_assignments", {
                    core.process_assignments(assignments, &mut notifications);
                    trace_time!("core", "notify", {
                        comm_ref.get_mut().notify(&mut core, notifications).unwrap();
                    });
                });
            }
            FromSchedulerMessage::Register(_) => {
                return Err(Error::SchedulerError(
                    "Double registration of scheduler".to_owned(),
                ));
            }
        }
    }

    Ok(())
}
