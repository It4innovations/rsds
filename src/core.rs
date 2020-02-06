use std::rc::Rc;

use crate::client::{Client, ClientId};
use crate::comm::Notifications;
use crate::common::{IdCounter, Identifiable, KeyIdMap, Map, WrappedRcRefCell};
use crate::protocol::workermsg::{StealResponseMsg, TaskFinishedMsg, WorkerState};
use crate::scheduler::schedproto::{TaskAssignment, TaskId, WorkerId};

use crate::protocol::key::{dask_key_ref_to_string, DaskKey, DaskKeyRef};
use crate::task::{DataInfo, ErrorInfo, Task, TaskRef, TaskRuntimeState};
use crate::worker::WorkerRef;

impl Identifiable for Client {
    type Id = ClientId;
    type Key = DaskKey;

    #[inline]
    fn get_id(&self) -> Self::Id {
        self.id()
    }

    #[inline]
    fn get_key(&self) -> Self::Key {
        self.key().into()
    }
}
impl Identifiable for WorkerRef {
    type Id = WorkerId;
    type Key = DaskKey;

    #[inline]
    fn get_id(&self) -> Self::Id {
        self.get().id()
    }

    #[inline]
    fn get_key(&self) -> Self::Key {
        self.get().key().into()
    }
}

pub struct Core {
    tasks_by_id: Map<TaskId, TaskRef>,
    tasks_by_key: Map<DaskKey, TaskRef>,
    clients: KeyIdMap<ClientId, Client, DaskKey>,
    workers: KeyIdMap<WorkerId, WorkerRef, DaskKey>,

    task_id_counter: IdCounter,
    worker_id_counter: IdCounter,
    client_id_counter: IdCounter,
    uid: DaskKey,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl Core {
    pub fn new() -> Self {
        Self {
            tasks_by_id: Default::default(),
            tasks_by_key: Default::default(),

            task_id_counter: Default::default(),
            worker_id_counter: Default::default(),
            client_id_counter: Default::default(),

            workers: KeyIdMap::new(),
            clients: KeyIdMap::new(),

            uid: "123_TODO".into(),
        }
    }

    #[inline]
    pub fn uid(&self) -> &DaskKeyRef {
        &self.uid
    }

    #[inline]
    pub fn new_client_id(&mut self) -> ClientId {
        self.client_id_counter.next()
    }

    #[inline]
    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter.next()
    }

    #[inline]
    pub fn new_task_id(&mut self) -> TaskId {
        self.task_id_counter.next()
    }

    #[inline]
    pub fn register_worker(&mut self, worker_ref: WorkerRef) {
        self.workers.insert(worker_ref);
    }

    #[inline]
    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        // TODO: send to scheduler
        self.workers.remove_by_id(worker_id);
    }

    #[inline]
    pub fn register_client(&mut self, client: Client) {
        self.clients.insert(client);
    }

    #[inline]
    pub fn unregister_client(&mut self, client_id: ClientId) {
        // TODO: remove tasks of this client
        self.clients.remove_by_id(client_id);
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let (task_id, task_key) = {
            let task = task_ref.get();
            (task.id, task.key.clone())
        };
        self.tasks_by_id.insert(task_id, task_ref.clone());
        assert!(self.tasks_by_key.insert(task_key, task_ref).is_none());
    }

    pub fn remove_task(&mut self, task: &Task) {
        assert!(!task.has_consumers());
        assert!(!task.has_subscribed_clients());
        assert!(self.tasks_by_id.remove(&task.id).is_some());
        assert!(self.tasks_by_key.remove(&task.key).is_some());
    }

    #[inline]
    pub fn get_task_by_key_or_panic(&self, key: &DaskKeyRef) -> &TaskRef {
        self.tasks_by_key.get(key).unwrap()
    }

    #[inline]
    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks_by_id.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    #[inline]
    pub fn get_task_by_id(&self, id: TaskId) -> Option<&TaskRef> {
        self.tasks_by_id.get(&id)
    }

    #[inline]
    pub fn get_client_by_id_or_panic(&mut self, id: ClientId) -> &mut Client {
        self.clients.get_mut_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid client id={}", id);
        })
    }

    #[inline]
    pub fn get_client_id_by_key(&self, key: &DaskKeyRef) -> ClientId {
        self.clients.get_id_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid client key={:?}", key);
        })
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &WorkerRef {
        self.workers.get_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_worker_by_key_or_panic(&self, key: &DaskKeyRef) -> &WorkerRef {
        self.workers.get_by_key(key).unwrap_or_else(|| {
            panic!(
                "Asking for invalid worker key={}",
                dask_key_ref_to_string(key)
            );
        })
    }

    #[inline]
    pub fn get_worker_id_by_key(&self, key: &DaskKeyRef) -> WorkerId {
        self.workers.get_id_by_key(key).unwrap_or_else(|| {
            panic!(
                "Asking for invalid worker key={}",
                dask_key_ref_to_string(key)
            );
        })
    }

    #[inline]
    pub fn get_workers(&self) -> Vec<WorkerRef> {
        self.workers.values().cloned().collect()
    }

    pub fn get_worker_cores(&self) -> Map<DaskKey, u64> {
        self.workers
            .values()
            .map(|w| {
                let w = w.get();
                (w.key().into(), w.ncpus as u64)
            })
            .collect()
    }

    pub fn process_assignments(
        &mut self,
        assignments: Vec<TaskAssignment>,
        notifications: &mut Notifications,
    ) {
        log::debug!("Assignments from scheduler: {:?}", assignments);
        for assignment in assignments {
            let worker_ref = self
                .workers
                .get_by_id(assignment.worker)
                .expect("Worker from assignment not found")
                .clone();
            let task_ref = self.get_task_by_id_or_panic(assignment.task);

            let mut task = task_ref.get_mut();
            let state = match &task.state {
                TaskRuntimeState::Waiting | TaskRuntimeState::Scheduled(_) => {
                    if task.is_ready() {
                        log::debug!(
                            "Task task={} scheduled & assigned to worker={}",
                            assignment.task,
                            assignment.worker
                        );
                        notifications.compute_task_on_worker(worker_ref.clone(), task_ref.clone());
                        TaskRuntimeState::Assigned(worker_ref)
                    } else {
                        log::debug!(
                            "Task task={} scheduled to worker={}",
                            assignment.task,
                            assignment.worker
                        );
                        TaskRuntimeState::Scheduled(worker_ref)
                    }
                }
                TaskRuntimeState::Assigned(wref) => {
                    log::debug!(
                        "Task task={} scheduled to worker={}; stealing (1) from worker={}",
                        assignment.task,
                        assignment.worker,
                        wref.get().id
                    );
                    notifications.steal_task_from_worker(wref.clone(), task_ref.clone());
                    TaskRuntimeState::Stealing(wref.clone(), worker_ref)
                }
                TaskRuntimeState::Stealing(wref1, _) => {
                    log::debug!(
                        "Task task={} scheduled to worker={}; stealing (2) from worker={}",
                        assignment.task,
                        assignment.worker,
                        wref1.get().id
                    );
                    TaskRuntimeState::Stealing(wref1.clone(), worker_ref)
                }
                TaskRuntimeState::Finished(_, _)
                | TaskRuntimeState::Released
                | TaskRuntimeState::Error(_) => {
                    log::debug!("Rescheduling non-active task={}", assignment.task);
                    continue;
                }
            };
            task.state = state;
        }
    }

    pub fn on_steal_response(
        &mut self,
        worker_ref: &WorkerRef,
        msg: StealResponseMsg,
        notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_key_or_panic(&msg.key);
        let mut task = task_ref.get_mut();
        if task.is_done() {
            return;
        }
        let to_w = if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
            assert!(from_w == worker_ref);
            to_w.clone()
        } else {
            panic!("Invalid state of task when steal response occured");
        };

        // This needs to correspond with behavior in worker!
        let success = match msg.state {
            WorkerState::Waiting | WorkerState::Ready => true,
            _ => false,
        };
        notifications.task_steal_response(&worker_ref.get(), &to_w.get(), &task, success);
        if success {
            log::debug!("Task stealing was successful task={}", task.id);
            notifications.compute_task_on_worker(to_w.clone(), task_ref.clone());
            task.state = TaskRuntimeState::Assigned(to_w);
        } else {
            log::debug!("Task stealing was not successful task={}", task.id);
            task.state = TaskRuntimeState::Assigned(worker_ref.clone())
        }
    }

    pub fn on_task_error(
        &mut self,
        worker: &WorkerRef,
        task_key: DaskKey,
        error_info: ErrorInfo,
        mut notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_key_or_panic(&task_key).clone();
        let error_info = Rc::new(error_info);
        let task_refs = {
            assert!(task_ref.get().is_assigned_or_stealed_from(worker));
            self.on_task_error_helper(&task_ref, error_info.clone(), &mut notifications);
            task_ref.get().collect_consumers()
        };

        for task_ref in task_refs {
            {
                let task = task_ref.get();
                assert!(task.is_waiting() || task.is_scheduled());
            }
            self.on_task_error_helper(&task_ref, error_info.clone(), &mut notifications);
        }
    }

    pub fn on_tasks_transferred(
        &mut self,
        worker_ref: &WorkerRef,
        keys: Vec<DaskKey>,
        notifications: &mut Notifications,
    ) {
        let worker = worker_ref.get();
        for key in keys {
            let task_ref = self.get_task_by_key_or_panic(&key);
            let task = task_ref.get_mut();
            notifications.task_placed(&worker, &task);
            // TODO: Store that task result is on worker
        }
    }

    pub fn on_task_finished(
        &mut self,
        worker: &WorkerRef,
        msg: TaskFinishedMsg,
        notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_key_or_panic(&msg.key).clone();
        {
            {
                let mut task = task_ref.get_mut();
                log::debug!(
                    "Task id={} finished on worker={}",
                    task.id,
                    worker.get().id()
                );
                assert!(task.is_assigned_or_stealed_from(worker));
                task.state = TaskRuntimeState::Finished(
                    DataInfo {
                        size: msg.nbytes,
                        r#type: msg.r#type,
                    },
                    vec![worker.clone()],
                );
            }
            {
                let task = task_ref.get();
                for consumer in task.get_consumers() {
                    let mut t = consumer.get_mut();
                    assert!(t.is_waiting() || t.is_scheduled());
                    t.unfinished_inputs -= 1;
                    if t.unfinished_inputs == 0 {
                        let wr = match &t.state {
                            TaskRuntimeState::Scheduled(w) => Some(w.clone()),
                            _ => None,
                        };
                        if let Some(w) = wr {
                            t.state = TaskRuntimeState::Assigned(w.clone());
                            notifications.compute_task_on_worker(w, consumer.clone());
                        }
                    }
                }
                notifications.task_finished(&worker.get(), &task);
                self.unregister_as_consumer(&task_ref, notifications);
                self.notify_key_in_memory(&task_ref, notifications);
            }

            task_ref.get_mut().remove_data_if_possible(self, notifications);
        }
    }

    fn unregister_as_consumer(&mut self, task_ref: &TaskRef, notifications: &mut Notifications) {
        let task = task_ref.get();
        for input_id in &task.dependencies {
            let tr = self.get_task_by_id_or_panic(*input_id).clone();
            let mut t = tr.get_mut();
            assert!(t.remove_consumer(&task_ref));
            t.remove_data_if_possible(self, notifications);
        }
    }

    fn notify_key_in_memory(&mut self, task_ref: &TaskRef, notifications: &mut Notifications) {
        let task = task_ref.get();
        for &client_id in task.subscribed_clients() {
            notifications.notify_client_key_in_memory(client_id, task_ref.clone());
        }
    }

    fn on_task_error_helper(
        &mut self,
        task_ref: &TaskRef,
        error_info: Rc<ErrorInfo>,
        mut notifications: &mut Notifications,
    ) {
        task_ref.get_mut().state = TaskRuntimeState::Error(error_info);
        self.unregister_as_consumer(task_ref, &mut notifications);

        let task = task_ref.get();
        for client_id in task.subscribed_clients() {
            notifications.notify_client_about_task_error(*client_id, task_ref.clone());
        }
    }
}

impl CoreRef {
    pub fn new() -> Self {
        Self::wrap(Core::new())
    }
}

#[cfg(test)]
mod tests {
    use super::Core;
    use crate::client::Client;
    use crate::comm::{ClientNotification, Notifications};
    use crate::protocol::workermsg::Status;
    use crate::protocol::workermsg::TaskFinishedMsg;
    use crate::scheduler::schedproto::{TaskUpdate, TaskUpdateType};
    use crate::scheduler::ToSchedulerMessage;
    use crate::task::{ErrorInfo, TaskRuntimeState};
    use crate::test_util::{
        client, dummy_serialized, task_add, task_add_deps, task_assign, worker,
    };

    #[test]
    fn add_remove() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);
        assert_eq!(core.get_task_by_key_or_panic(&t.get().key), &t);

        core.remove_task(&t.get());
        assert_eq!(core.get_task_by_id(0), None);
    }

    #[test]
    fn assign_task() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);

        let (w, _w_rx) = worker(&mut core, "w0");
        let notifications = task_assign(&mut core, &t, &w);
        assert!(t.get().is_assigned_or_stealed_from(&w));
        assert_eq!(notifications.workers[&w].compute_tasks, vec!(t));
    }

    #[test]
    fn finish_task_scheduler_notification() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let key = t.get().key.clone();

        let mut notifications = Notifications::default();
        let nbytes = 16;
        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                status: Status::Ok,
                key,
                nbytes,
                r#type: vec![],
            },
            &mut notifications,
        );
        assert_eq!(
            notifications.scheduler_messages[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: t.get().id,
                state: TaskUpdateType::Finished,
                worker: w.get().id,
                size: Some(nbytes)
            })
        );
    }

    #[test]
    fn release_task_without_consumers() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let key = t.get().key.clone();
        let r#type = vec![1, 2, 3];
        let nbytes = 16;

        let mut notifications = Notifications::default();
        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                status: Status::Ok,
                key,
                nbytes,
                r#type: r#type.clone(),
            },
            &mut notifications,
        );
        match &t.get().state {
            TaskRuntimeState::Released => {}
            _ => panic!("Wrong task state"),
        };
    }

    #[test]
    fn finish_task_with_consumers() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);
        let _ = task_add_deps(&mut core, 1, &[&t]);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let key = t.get().key.clone();
        let r#type = vec![1, 2, 3];
        let nbytes = 16;

        let mut notifications = Notifications::default();
        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                status: Status::Ok,
                key,
                nbytes,
                r#type: r#type.clone(),
            },
            &mut notifications,
        );
        match &t.get().state {
            TaskRuntimeState::Finished(data, workers) => {
                assert_eq!(data.size, nbytes);
                assert_eq!(data.r#type, r#type);
                assert_eq!(workers, &vec!(w));
            }
            _ => panic!("Wrong task state"),
        };
    }

    #[test]
    fn finish_task_inmemory_notification() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let key = t.get().key.clone();
        let clients: Vec<Client> = (0..2)
            .map(|c| {
                let (client, _c_rx) = client(c);
                t.get_mut().subscribe_client(client.id());
                client
            })
            .collect();

        let mut notifications = Notifications::default();
        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                status: Status::Ok,
                key,
                nbytes: 0,
                r#type: vec![],
            },
            &mut notifications,
        );
        for client in clients {
            assert_eq!(
                notifications.clients[&client.id()],
                ClientNotification {
                    in_memory_tasks: vec![t.clone()],
                    error_tasks: vec![]
                }
            );
        }
    }

    #[test]
    #[should_panic]
    fn finish_unassigned_task() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);

        let (w, _w_rx) = worker(&mut core, "w0");

        let key = t.get().key.clone();
        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                status: Status::Ok,
                key,
                nbytes: 16,
                r#type: vec![1, 2, 3],
            },
            &mut &mut Default::default(),
        );
    }

    #[test]
    #[should_panic]
    fn error_unassigned_task() {
        let mut core = Core::new();
        let t = task_add(&mut core, 0);

        let (w, _w_rx) = worker(&mut core, "w0");

        let key = t.get().key.clone();
        core.on_task_error(
            &w,
            key,
            ErrorInfo {
                exception: dummy_serialized(),
                traceback: dummy_serialized(),
            },
            &mut Default::default(),
        );
    }
}
