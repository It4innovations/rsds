use smallvec::SmallVec;

use crate::common::{IdCounter, Map, WrappedRcRefCell};
use crate::scheduler::TaskId;
use crate::server::dask::client::{Client, ClientId};
use crate::server::dask::gateway::DaskGateway;
use crate::server::dask::key::DaskKey;
use crate::server::gateway::Gateway;

pub struct DaskState {
    uid: DaskKey,

    clients: Map<ClientId, Client>,

    task_id_to_key: Map<TaskId, DaskKey>,
    task_key_to_id: Map<DaskKey, TaskId>,

    subscriptions: Map<TaskId, SmallVec<[ClientId; 1]>>,
    client_id_counter: IdCounter,
}

pub type DaskStateRef = WrappedRcRefCell<DaskState>;

impl DaskState {
    #[inline]
    pub fn uid(&self) -> &DaskKey {
        &self.uid
    }

    #[inline]
    pub fn get_task_id(&self, key: &DaskKey) -> Option<TaskId> {
        self.task_key_to_id.get(key).copied()
    }

    pub fn get_all_keys(&self) -> impl Iterator<Item = &DaskKey> {
        self.task_key_to_id.keys()
    }

    #[inline]
    pub fn subscriptions(&self) -> &Map<TaskId, SmallVec<[ClientId; 1]>> {
        &self.subscriptions
    }

    pub fn remove_all_subscriptions(&mut self, task_id: TaskId) -> Option<SmallVec<[ClientId; 1]>> {
        self.subscriptions.remove(&task_id)
    }

    pub fn remove_task_id(&mut self, task_id: TaskId) {
        let key = self.task_id_to_key.remove(&task_id).unwrap();
        assert!(self.task_key_to_id.remove(&key).is_some());
    }

    #[inline]
    pub fn get_task_key(&self, task_id: TaskId) -> Option<&DaskKey> {
        self.task_id_to_key.get(&task_id)
    }

    #[inline]
    pub fn new_client_id(&mut self) -> ClientId {
        self.client_id_counter.next()
    }

    pub fn insert_task_key(&mut self, task_key: DaskKey, task_id: TaskId) {
        assert!(self
            .task_key_to_id
            .insert(task_key.clone(), task_id)
            .is_none());
        assert!(self.task_id_to_key.insert(task_id, task_key).is_none());
    }

    pub fn subscribe_client_to_task(&mut self, task_id: TaskId, client_id: ClientId) {
        log::debug!("Subscribing task={} to client={}", task_id, client_id);
        self.subscriptions
            .entry(task_id)
            .or_default()
            .push(client_id);
    }

    pub fn unsubscribe_client_from_task(&mut self, task_id: TaskId, client_id: ClientId) -> bool {
        log::debug!("Unsubscribing task={} from client={}", task_id, client_id);
        let remove = self
            .subscriptions
            .get_mut(&task_id)
            .map(|v| {
                v.iter().position(|c| *c == client_id).map(|p| v.remove(p));
                v.is_empty()
            })
            .unwrap_or(false);

        if remove {
            self.subscriptions.remove(&task_id).is_some()
        } else {
            false
        }
    }

    pub fn register_client(&mut self, client: Client) {
        let client_id = client.id();
        self.clients.insert(client_id, client);
    }

    pub fn unregister_client(&mut self, client_id: ClientId) {
        // TODO: remove tasks of this client
        assert!(self.clients.remove(&client_id).is_some());
    }

    #[inline]
    pub fn get_client_by_id_or_panic(&self, id: ClientId) -> &Client {
        self.clients.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid client id={}", id);
        })
    }

    pub fn get_client_by_key(&self, key: &DaskKey) -> Option<&Client> {
        self.clients.values().find(|client| client.key() == key)
    }

    pub fn dump_mapping(&self) {
        for (k, v) in &self.task_id_to_key {
            log::debug!("{} -> {}", k, v);
        }
    }
}

impl DaskStateRef {
    pub fn new() -> Self {
        DaskStateRef::wrap(DaskState {
            uid: "123_TODO".into(),
            task_id_to_key: Default::default(),
            task_key_to_id: Default::default(),
            subscriptions: Default::default(),
            clients: Default::default(),
            client_id_counter: IdCounter::default(),
        })
    }

    pub fn get_gateway(&self) -> Box<dyn Gateway> {
        Box::new(DaskGateway {
            dask_state_ref: self.clone(),
        })
    }
}
