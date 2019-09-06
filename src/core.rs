use crate::common::WrappedRcRefCell;
use crate::prelude::*;
use std::collections::HashMap;

pub struct Core {
    tasks: HashMap<String, Rc<Task>>,
    clients: HashMap<ClientId, Client>,
    uid: String,

    workers: HashMap<WorkerId, WorkerRef>,
    client_id_counter: ClientId,
    worker_id_counter: ClientId,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl Core {
    pub fn register_client(&mut self, client: Client) {
        assert!(self.clients.insert(client.id(), client).is_none());
    }

    pub fn new_client_id(&mut self) -> ClientId {
        self.client_id_counter += 1;
        self.client_id_counter
    }

    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter += 1;
        self.worker_id_counter
    }

    pub fn register_worker(&mut self, worker_ref: WorkerRef) {
        let worker_id = worker_ref.get().id();
        assert!(self.workers.insert(worker_id, worker_ref).is_none());
    }

    pub fn unregister_client(&mut self, client_id: ClientId) {
        assert!(self.clients.remove(&client_id).is_some());
    }

    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        assert!(self.workers.remove(&worker_id).is_some());
    }

    pub fn add_task(&mut self, task_ref: Rc<Task>) {
        assert!(self.tasks.insert(task_ref.key.clone(), task_ref).is_none());
    }

    pub fn get_task_or_panic(&mut self, key: &str) -> &mut Rc<Task> {
        self.tasks.get_mut(key).unwrap()
    }
}

impl CoreRef {
    pub fn new() -> Self {
        Self::wrap(Core {
            tasks: Default::default(),

            worker_id_counter: 0,
            workers: Default::default(),

            client_id_counter: 0,
            clients: Default::default(),

            uid: "123_TODO".into(),
        })
    }

    pub fn uid(&self) -> String {
        self.get().uid.clone()
    }
}
