use std::rc::Rc;

use futures::stream::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::client::{Client, ClientId};
use crate::common::{IdCounter, Identifiable, KeyIdMap, Map, WrappedRcRefCell};
use crate::notifications::Notifications;
use crate::protocol::workermsg::{StealResponseMsg, TaskFinishedMsg, WorkerState};
use crate::scheduler::schedproto::{TaskAssignment, TaskId, WorkerId};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::task::{DataInfo, ErrorInfo, Task, TaskKey, TaskRef, TaskRuntimeState};
use crate::worker::WorkerRef;

impl Identifiable for Client {
    type Id = ClientId;
    type Key = String;

    #[inline]
    fn get_id(&self) -> Self::Id {
        self.id()
    }

    #[inline]
    fn get_key(&self) -> Self::Key {
        self.key().to_owned()
    }
}
impl Identifiable for WorkerRef {
    type Id = WorkerId;
    type Key = String;

    #[inline]
    fn get_id(&self) -> Self::Id {
        self.get().id()
    }

    #[inline]
    fn get_key(&self) -> Self::Key {
        self.get().key().to_owned()
    }
}

pub struct Core {
    tasks_by_id: Map<TaskId, TaskRef>,
    tasks_by_key: Map<TaskKey, TaskRef>,
    clients: KeyIdMap<ClientId, Client>,
    workers: KeyIdMap<WorkerId, WorkerRef>,

    task_id_counter: IdCounter,
    worker_id_counter: IdCounter,
    client_id_counter: IdCounter,
    uid: String,

    // This is reference to itself
    // For real cleanup you have to remove this cycle
    // However, since Core practically global object, it is never done
    self_ref: Option<CoreRef>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl Core {
    #[inline]
    pub fn uid(&self) -> &str {
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
        assert!(task.consumers.is_empty());
        self.tasks_by_id.remove(&task.id);
        self.tasks_by_key.remove(&task.key);
    }

    #[inline]
    pub fn get_task_by_key_or_panic(&self, key: &str) -> &TaskRef {
        self.tasks_by_key.get(key).unwrap()
    }

    #[inline]
    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks_by_id.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    #[inline]
    pub fn get_client_by_id_or_panic(&mut self, id: ClientId) -> &mut Client {
        self.clients.get_mut_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid client id={}", id);
        })
    }

    #[inline]
    pub fn get_client_id_by_key(&self, key: &str) -> ClientId {
        self.clients.get_id_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid client key={}", key);
        })
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &WorkerRef {
        self.workers.get_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_worker_by_key_or_panic(&self, key: &str) -> &WorkerRef {
        self.workers.get_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid worker key={}", key);
        })
    }

    #[inline]
    pub fn get_worker_id_by_key(&self, key: &str) -> WorkerId {
        self.workers.get_id_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid worker key={}", key);
        })
    }

    #[inline]
    pub fn get_workers(&self) -> Vec<WorkerRef> {
        self.workers.values().cloned().collect()
    }

    pub fn get_worker_cores(&self) -> Map<String, u64> {
        self.workers
            .values()
            .map(|w| {
                let w = w.get();
                (w.key().to_owned(), w.ncpus as u64)
            })
            .collect()
    }

    pub fn process_assignments(&mut self, assignments: Vec<TaskAssignment>, notifications: &mut Notifications) {
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
                },
                TaskRuntimeState::Finished(_, _)
                | TaskRuntimeState::Released(_)
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
        _worker: &WorkerRef,
        task_key: TaskKey,
        error_info: ErrorInfo,
        mut notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_key_or_panic(&task_key).clone();
        let error_info = Rc::new(error_info);
        let task_refs = {
            assert!(task_ref.get().is_assigned());
            self.on_task_error_helper(
                &task_ref,
                error_info.clone(),
                &mut notifications,
            );
            task_ref.get().collect_consumers()
        };

        for task_ref in task_refs {
            {
                let task = task_ref.get();
                assert!(task.is_waiting() || task.is_scheduled());
            }
            self.on_task_error_helper(
                &task_ref,
                error_info.clone(),
                &mut notifications,
            );
        }
    }

    pub fn unregister_as_consumer(
        &mut self,
        task_ref: &TaskRef,
        notifications: &mut Notifications,
    ) {
        let task = task_ref.get();
        for input_id in &task.dependencies {
            let tr = self.get_task_by_id_or_panic(*input_id);
            let mut t = tr.get_mut();
            assert!(t.consumers.remove(&task_ref));
            t.remove_data_if_possible(notifications);
        }
    }

    pub fn on_tasks_transferred(
        &mut self,
        worker_ref: &WorkerRef,
        keys: Vec<String>,
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
                for consumer in &task.consumers {
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

            task_ref.get_mut().remove_data_if_possible(notifications);
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
        let core_ref = Self::wrap(Core {
            tasks_by_id: Default::default(),
            tasks_by_key: Default::default(),

            task_id_counter: Default::default(),
            worker_id_counter: Default::default(),
            client_id_counter: Default::default(),

            workers: KeyIdMap::new(),
            clients: KeyIdMap::new(),

            uid: "123_TODO".into(),
            self_ref: None,
        });
        {
            let mut core = core_ref.get_mut();
            core.self_ref = Some(core_ref.clone());
        }
        core_ref
    }
}
