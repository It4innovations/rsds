use std::cmp::Reverse;

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use tokio::sync::mpsc::UnboundedSender;

use crate::common::WrappedRcRefCell;
use crate::server::protocol::{PriorityValue, Priority};
use crate::worker::subworker::{SubworkerId, SubworkerRef};
use crate::worker::task::{TaskRef, Task, TaskState};
use crate::worker::data::{DataObjectRef, DataObjectState, LocalData, RemoteData};
use crate::server::protocol::key::DaskKey;
use crate::common::data::SerializationType;
use crate::worker::reactor::choose_subworker;
use crate::server::protocol::messages::worker::{FromWorkerMessage, DataDownloadedMsg, StealResponse};

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<Bytes>,
    pub ncpus: u32,
    pub listen_address: String,
    pub subworkers: HashMap<SubworkerId, SubworkerRef>,
    pub free_subworkers: Vec<SubworkerRef>,
    pub tasks: HashMap<DaskKey, TaskRef>,
    pub ready_task_queue: priority_queue::PriorityQueue<TaskRef, Reverse<(PriorityValue, PriorityValue)>>,
    pub data_objects: HashMap<DaskKey, DataObjectRef>,
    pub download_sender: tokio::sync::mpsc::UnboundedSender<(DataObjectRef, Priority)>,
}


impl WorkerState {
    pub fn set_subworkers(&mut self, subworkers: Vec<SubworkerRef>) {
        assert!(self.subworkers.is_empty() && self.free_subworkers.is_empty());
        self.free_subworkers = subworkers.clone();
        self.subworkers = subworkers
            .iter()
            .map(|s| {
                let id = s.get().id;
                (id, s.clone())
            })
            .collect();
    }

    pub fn add_data_object(&mut self, data_ref: DataObjectRef) {
        let key = data_ref.get().key.clone();
        self.data_objects.insert(key, data_ref);
    }

    pub fn send_message_to_server(&self, data: Vec<u8>) {
        self.sender.send(data.into()).unwrap();
    }

    pub fn on_data_downloaded(&mut self, data_ref: DataObjectRef, data: BytesMut, serializer: SerializationType) {
        {
            let mut data_obj = data_ref.get_mut();
            log::debug!("Data {} downloaded ({} bytes)", data_obj.key, data.len());
            match data_obj.state {
                DataObjectState::Remote(_) => { /* This is ok */ },
                DataObjectState::Removed => {
                    /* download was completed, but we do not care about data */
                    log::debug!("Data is not needed any more");
                    return;
                }
                DataObjectState::Local(_) => { unreachable!() }
            }
            data_obj.state = DataObjectState::Local(LocalData {
                serializer,
                bytes: data.into(),
            });

            let message = FromWorkerMessage::DataDownloaded(DataDownloadedMsg {
                key: data_obj.key.clone()
            });
            self.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());
        }

        /* TODO: Inform server about download */

        /* We need to reborrow the data_ref to readonly as
           add_ready_task may start to read this data_ref
         */
        for task_ref in &data_ref.get().consumers {
            let is_ready = task_ref.get_mut().decrease_waiting_count();
            if is_ready {
                log::debug!("Task {} becomes ready", task_ref.get().key);
                self.add_ready_task(task_ref.clone());
            }
        }
    }

    pub fn add_ready_task(&mut self, task_ref: TaskRef) {
        let priority = task_ref.get().priority.clone();
        self.ready_task_queue.push(task_ref, Reverse(priority));
        self.try_start_tasks();
    }

    pub fn add_dependancy(&mut self, task_ref: &TaskRef, key: DaskKey, size: u64, workers: Vec<DaskKey>) {
        let mut task = task_ref.get_mut();
        let mut is_remote = false;
        let data_ref = match self.data_objects.get(&key).cloned() {
            None => {
                let data_ref = DataObjectRef::new(key.clone(), size, DataObjectState::Remote(RemoteData {
                    workers
                }));
                self.data_objects.insert(key, data_ref.clone());
                is_remote = true;
                data_ref

            }
            Some(data_ref) => {
                {
                    let mut data_obj = data_ref.get_mut();
                    match data_obj.state {
                        DataObjectState::Remote(_) => {
                            is_remote = true;
                            data_obj.state = DataObjectState::Remote(RemoteData {
                                workers
                            })
                        }
                        DataObjectState::Local(_) => { /* Do nothing */ }
                        DataObjectState::Removed => { unreachable!(); }
                    };
                }
                data_ref
            }
        };
        data_ref.get_mut().consumers.insert(task_ref.clone());
        if is_remote {
            task.increase_waiting_count();
            let _ = self.download_sender.send((data_ref.clone(), task.priority));
        }
        task.deps.push(data_ref);
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let key = task_ref.get().key.clone();
        if task_ref.get().is_ready() {
            log::debug!("Task {} is directly ready", task_ref.get().key);
            self.add_ready_task(task_ref.clone());
        } else {
            let task = task_ref.get();
            log::debug!("Task {} is blocked by {} remote objects", task.key, task.get_waiting());
        }
        self.tasks.insert(key, task_ref);
    }

    pub fn try_start_tasks(&mut self) {
        if self.free_subworkers.is_empty() {
            return;
        }
        while let Some((task_ref, _)) = self.ready_task_queue.pop() {
            {
                let subworker_ref = choose_subworker(self);
                let mut task = task_ref.get_mut();
                task.set_running(subworker_ref.clone());
                let mut sw = subworker_ref.get_mut();
                assert!(sw.running_task.is_none());
                sw.running_task = Some(task_ref.clone());
                sw.start_task(&task);
            }
            if self.free_subworkers.is_empty() {
                return;
            }
        }
    }

    pub fn remove_data(&mut self, key: &DaskKey) {
        log::info!("Removing data object {}", key);
        self.data_objects.remove(key).map(|data_ref| {
            let mut data_obj = data_ref.get_mut();
            data_obj.state = DataObjectState::Removed;
            if !data_obj.consumers.is_empty() {
                todo!(); // What should happen when server removes data but there are tasks that needs it?
            }
        });
    }

    /*
    pub fn complete_task(&mut self, task_ref: TaskRef) {
        assert!(task_ref.get().is_running());
        //self._remove_task(task_ref);
    }*/

    pub fn remove_task(&mut self, task_ref: TaskRef, just_finished: bool) {
        let mut task = task_ref.get_mut();
        match task.state {
            TaskState::Waiting(x) => {
                assert!(!just_finished);
                if x == 0 {
                    assert!(self.ready_task_queue.remove(&task_ref).is_some());
                }
            }
            TaskState::Running(_) => {
                assert!(just_finished);
            }
            TaskState::Removed => {
                unreachable!();
            }
        }
        task.state = TaskState::Removed;

        assert!(self.tasks.remove(&task.key).is_some());

        for data_ref in std::mem::take(&mut task.deps) {
            let mut data = data_ref.get_mut();
            assert!(data.consumers.remove(&task_ref));
            if data.consumers.is_empty() {
                match data.state {
                    DataObjectState::Remote(_) => {
                        assert!(!just_finished);
                        data.state = DataObjectState::Removed;
                    }
                    DataObjectState::Local(_) => { /* Do nothing */ }
                    DataObjectState::Removed => { /* Do nothing */ }
                };
            }
        }
    }

    pub fn steal_task(&mut self, key: &DaskKey) -> StealResponse {
        match self.tasks.get(key).cloned() {
            None => StealResponse::NotHere,
            Some(task_ref) => {
                {
                    let mut task = task_ref.get_mut();
                    match task.state {
                        TaskState::Waiting(_) => { /* Continue */ },
                        TaskState::Running(_) => return StealResponse::Running,
                        TaskState::Removed => return StealResponse::NotHere,
                    }
                }
                self.remove_task(task_ref, false);
                StealResponse::Ok
            }
        }
    }

}

impl WorkerStateRef {
    pub fn new(
        sender: UnboundedSender<Bytes>,
        ncpus: u32,
        listen_address: String,
        download_sender: tokio::sync::mpsc::UnboundedSender<(DataObjectRef, Priority)>
    ) -> Self {
        Self::wrap(WorkerState {
            sender,
            ncpus,
            listen_address,
            download_sender,
            tasks: Default::default(),
            subworkers: Default::default(),
            free_subworkers: Default::default(),
            ready_task_queue: Default::default(),
            data_objects: Default::default(),
        })
    }
}
