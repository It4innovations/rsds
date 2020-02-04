use std::rc::Rc;

use futures::stream::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::client::{Client, ClientId};
use crate::common::{Identifiable, KeyIdMap, Map, WrappedRcRefCell};
use crate::notifications::Notifications;
use crate::protocol::clientmsg::{KeyInMemoryMsg, ToClientMessage};
use crate::protocol::workermsg::{TaskFinishedMsg, StealResponseMsg, WorkerState};
use crate::scheduler::schedproto::{TaskAssignment, TaskId, WorkerId};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::task::{DataInfo, ErrorInfo, Task, TaskKey, TaskRef, TaskRuntimeState};
use crate::worker::WorkerRef;
use crate::task::TaskRuntimeState::Scheduled;

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

    scheduler_sender: UnboundedSender<Vec<ToSchedulerMessage>>,

    id_counter: u64,
    uid: String,

    // This is reference to itself
    // For real cleanup you have to remove this cycle
    // However, since Core practically global object, it is never done
    self_ref: Option<CoreRef>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl Core {
    pub fn register_client(&mut self, client: Client) {
        self.clients.insert(client);
    }

    #[inline]
    fn new_id(&mut self) -> u64 {
        self.id_counter += 1;
        self.id_counter
    }

    pub fn new_client_id(&mut self) -> ClientId {
        self.new_id()
    }

    pub fn new_worker_id(&mut self) -> WorkerId {
        self.new_id()
    }

    pub fn new_task_id(&mut self) -> TaskId {
        self.new_id()
    }

    pub fn register_worker(&mut self, worker_ref: WorkerRef, notifications: &mut Notifications) {
        notifications.new_worker(&worker_ref.get());
        self.workers.insert(worker_ref);
    }

    #[inline]
    pub fn unregister_client(&mut self, client_id: ClientId) {
        self.clients.remove_by_id(client_id);
    }

    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        self.workers.remove_by_id(worker_id);
    }

    // ! This function modifies update, but do not triggers, send_update
    // You have to done it manually.
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

    pub fn get_task_by_key_or_panic(&self, key: &str) -> &TaskRef {
        self.tasks_by_key.get(key).unwrap()
    }

    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks_by_id.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    pub fn get_client_by_id_or_panic(&mut self, id: ClientId) -> &mut Client {
        self.clients.get_mut_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid client id={}", id);
        })
    }

    pub fn get_client_id_by_key(&self, key: &str) -> ClientId {
        self.clients.get_id_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid client key={}", key);
        })
    }

    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &WorkerRef {
        self.workers.get_by_id(id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    pub fn get_worker_by_key_or_panic(&self, key: &str) -> &WorkerRef {
        self.workers.get_by_key(key).unwrap_or_else(|| {
            panic!("Asking for invalid worker key={}", key);
        })
    }

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
                (w.listen_address.clone(), w.ncpus as u64)
            })
            .collect()
    }

    /*
    fn _send_scheduler_update_now(&mut self) {
        log::debug!("Sending update to scheduler");
        let update = std::mem::take(&mut self.update);
        let msg = ToSchedulerMessage::Update(update);
        self.scheduler_sender.try_send(msg).unwrap();
    }

    pub fn send_scheduler_update(&mut self, aggregate: bool) {
        if self.update_timeout_running {
            return;
        }
        if !aggregate {
            self._send_scheduler_update_now();
            return;
        }
        self.update_timeout_running = true;
        let core_ref = self.new_self_ref();
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(25))
            .unwrap();
        current_thread::spawn(tokio::timer::delay(deadline).map(move |()| {
            let mut core = core_ref.get_mut();
            core.update_timeout_running = false;
            core._send_scheduler_update_now();
        }));
    }*/

    pub fn send_scheduler_messages(&mut self, messages: Vec<ToSchedulerMessage>) {
        self.scheduler_sender.send(messages).unwrap();
    }

    pub fn new_self_ref(&self) -> CoreRef {
        self.self_ref.clone().unwrap()
    }

    pub fn process_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        log::debug!("Assignments from scheduler: {:?}", assignments);
        let mut notifications = Notifications::default();
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
                },
                TaskRuntimeState::Assigned(wref) => {
                    log::debug!("Task task={} scheduled to worker={}; stealing (1) from worker={}", assignment.task, assignment.worker, wref.get().id);
                    notifications.steal_task_from_worker(wref.clone(), task_ref.clone());
                    TaskRuntimeState::Stealing(wref.clone(), worker_ref)
                },
                TaskRuntimeState::Stealing(wref1, _) => {
                    log::debug!("Task task={} scheduled to worker={}; stealing (2) from worker={}", assignment.task, assignment.worker, wref1.get().id);
                    TaskRuntimeState::Stealing(wref1.clone(), worker_ref)
                },
                TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Released(_) | TaskRuntimeState::Error(_) => {
                    log::debug!("Rescheduling non-active task={}", assignment.task);
                    continue;
                }
            };
            task.state = state;
        }
        notifications.send(self).unwrap();
    }

    pub fn _on_task_error_helper(
        &mut self,
        task: &mut Task,
        task_ref: &TaskRef,
        error_info: Rc<ErrorInfo>,
        mut notifications: &mut Notifications,
    ) {
        task.state = TaskRuntimeState::Error(error_info);
        self.unregister_as_consumer(&task, task_ref, &mut notifications);
        for client_id in task.subscribed_clients() {
            notifications.notify_client_about_task_error(*client_id, task_ref.clone());
        }
    }

    pub fn on_steal_response(&mut self, worker_ref: &WorkerRef, msg: StealResponseMsg, mut notifications: &mut Notifications) {
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
            _ => false
        };
        notifications.task_steal_response(&worker_ref.get(), &to_w.get(), &task, success);
        if success {
            log::debug!("Task stealing was successful task={}", task.id);
            notifications.compute_task_on_worker(to_w.clone(), task_ref.clone());
            TaskRuntimeState::Assigned(to_w);
            task.state = TaskRuntimeState::Waiting
        } else {
            log::debug!("Task stealing was not successful task={}", task.id);
            task.state = TaskRuntimeState::Assigned(worker_ref.clone())
        }
        // TODO: Notify scheduler
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
            let mut task = task_ref.get_mut();
            assert!(task.is_assigned());
            self._on_task_error_helper(
                &mut task,
                &task_ref,
                error_info.clone(),
                &mut notifications,
            );
            task.collect_consumers()
        };

        for task_ref in task_refs {
            let mut task = task_ref.get_mut();
            assert!(task.is_waiting() || task.is_scheduled());
            self._on_task_error_helper(
                &mut task,
                &task_ref,
                error_info.clone(),
                &mut notifications,
            );
        }
    }

    pub fn unregister_as_consumer(
        &mut self,
        task: &Task,
        task_ref: &TaskRef,
        notifications: &mut Notifications,
    ) {
        for input_id in &task.dependencies {
            let tr = self.get_task_by_id_or_panic(*input_id);
            let mut t = tr.get_mut();
            assert!(t.consumers.remove(&task_ref));
            t.check_if_data_cannot_be_removed(notifications);
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
        mut notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_key_or_panic(&msg.key).clone();
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
            self.unregister_as_consumer(&task, &task_ref, &mut notifications);
            self.notify_key_in_memory(&task);
            task.check_if_data_cannot_be_removed(notifications);
        }
    }

    fn notify_key_in_memory(&mut self, task: &Task) {
        for client_id in task.subscribed_clients() {
            match self.clients.get_mut_by_id(*client_id) {
                Some(client) => {
                    client
                        .send_message(ToClientMessage::KeyInMemory(KeyInMemoryMsg {
                            key: task.key.clone(),
                            r#type: task.data_info().unwrap().r#type.clone(),
                        }))
                        .unwrap();
                }
                None => {
                    panic!(
                        "Task id={} finished for a dropped client={}",
                        task.id, client_id
                    );
                }
            }
        }
    }
}

impl CoreRef {
    pub fn new(scheduler_sender: UnboundedSender<Vec<ToSchedulerMessage>>) -> Self {
        let core_ref = Self::wrap(Core {
            tasks_by_id: Default::default(),
            tasks_by_key: Default::default(),

            id_counter: 0,

            workers: KeyIdMap::new(),
            clients: KeyIdMap::new(),

            scheduler_sender,

            uid: "123_TODO".into(),
            self_ref: None,
        });
        {
            let mut core = core_ref.get_mut();
            core.self_ref = Some(core_ref.clone());

            // Prepare default guess of net bandwidth [MB/s], TODO: better guess,
            // It will be send with the first update message.
            core.send_scheduler_messages(vec![ToSchedulerMessage::NetworkBandwidth(100.0)]);
        }
        core_ref
    }

    pub async fn observe_scheduler(self, mut recv: UnboundedReceiver<FromSchedulerMessage>) {
        log::debug!("Starting scheduler");

        match recv.next().await {
            Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
                log::debug!("Scheduler registered: {:?}", r);
            }
            None => {
                panic!("Scheduler closed connection without registration");
            }
            _ => {
                panic!("First message of scheduler has to be registration");
            }
        }

        while let Some(msg) = recv.next().await {
            match msg {
                FromSchedulerMessage::TaskAssignments(assignments) => {
                    let mut core = self.get_mut();
                    core.process_assignments(assignments);
                }
                FromSchedulerMessage::Register(_) => {
                    panic!("Double registration of scheduler");
                }
            }
        }
    }

    pub fn uid(&self) -> String {
        self.get().uid.clone()
    }
}
