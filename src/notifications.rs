use std::collections::HashMap;

use bytes::Bytes;

use crate::client::ClientId;
use crate::core::Core;
use crate::daskcodec::DaskMessage;
use crate::messages::aframe::{AfDescriptor, AfHeader, AfKeyElement, MessageBuilder};
use crate::messages::clientmsg::{TaskErredMsg, ToClientMessage};
use crate::messages::workermsg::{DeleteDataMsg, ToWorkerMessage};
use crate::task::{TaskKey, TaskRef};
use crate::task::TaskRuntimeState;
use crate::worker::WorkerRef;

#[derive(Default)]
pub struct WorkerNotification {
    pub compute_tasks: Vec<TaskRef>,
    pub delete_keys: Vec<TaskKey>,
}


#[derive(Default)]
pub struct ClientNotification {
    pub error_tasks: Vec<TaskRef>,
}


pub struct Notifications {
    workers: HashMap<WorkerRef, WorkerNotification>,
    clients: HashMap<ClientId, ClientNotification>,
}

impl Notifications {
    pub fn new() -> Self {
        Notifications {
            workers: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    pub fn delete_key_from_worker(&mut self, worker_ref: WorkerRef, key: &str) {
        self.workers.entry(worker_ref).or_default().delete_keys.push(key.to_string());
    }

    pub fn compute_task_on_worker(&mut self, worker_ref: WorkerRef, task_ref: TaskRef) {
        self.workers.entry(worker_ref).or_default().compute_tasks.push(task_ref);
    }

    pub fn notify_client_about_task_error(&mut self, client_id: ClientId, task_ref: TaskRef) {
        self.clients.entry(client_id).or_default().error_tasks.push(task_ref);
    }

    pub fn send(self, core: &mut Core) {
        /* Send to workers */
        for (worker_ref, w_update) in self.workers {
            let mut mbuilder = MessageBuilder::new();

            for task in w_update.compute_tasks {
                task.get().make_compute_task_msg(core, &mut mbuilder);
            }

            if !w_update.delete_keys.is_empty() {
                mbuilder.add_message(ToWorkerMessage::DeleteData(DeleteDataMsg { keys: w_update.delete_keys, report: false }));
            }
            if !mbuilder.is_empty() {
                let mut worker = worker_ref.get_mut();
                worker.send_dask_message(mbuilder.build()).unwrap_or_else(|_| {
                    // !!! Do not propagate error right now, we need to finish sending messages to others
                    // Worker cleanup is done elsewhere (when worker future terminates),
                    // so we can safely ignore this. Since we are nice guys we log (debug) message.
                    log::debug!("Sending tasks to worker {} failed", worker.id);
                });
            }
        }

        /* Send to clients */
        for (client_id, c_update) in self.clients {
            if !c_update.error_tasks.is_empty() {
                let mut frames: Vec<Bytes> = Vec::with_capacity(c_update.error_tasks.len() * 2 + 1);
                frames.push(Default::default()); // Reserve place for header
                let mut messages: Vec<ToClientMessage> = Vec::with_capacity(c_update.error_tasks.len());

                let mut descriptor: AfDescriptor = Default::default();

                for (i, task_ref) in c_update.error_tasks.iter().enumerate() {
                    let task = task_ref.get();
                    if let TaskRuntimeState::Error(e) = &task.state {
                        messages.push(ToClientMessage::TaskErred(TaskErredMsg {
                            key: task.key.clone(),
                        }));
                        for af in &e.frames {
                            let mut key = af.key.clone();
                            key[0] = AfKeyElement::Index(i as u64);
                            descriptor.keys.push(key.clone());
                            descriptor.headers.push((key, af.header.clone()));
                            frames.push(af.data.clone().into());
                        };
                    } else {
                        panic!("Task is not in error state");
                    };
                };
                let message = rmp_serde::encode::to_vec_named(&messages).unwrap();
                frames[0] = rmp_serde::encode::to_vec_named(&descriptor).unwrap().into();
                let mut client = core.get_client_by_id_or_panic(client_id);
                client.send_dask_message(DaskMessage::new(message.into(), frames));
            }
        }
    }
}
