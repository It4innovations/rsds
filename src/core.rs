use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::FutureExt;
use futures::stream::StreamExt;
use tokio::runtime::current_thread;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::common::WrappedRcRefCell;
use crate::messages::clientmsg::{KeyInMemoryMsg, ToClientMessage};
use crate::messages::workermsg::{TaskFinishedMsg, ToWorkerMessage};
use crate::notifications::Notifications;
use crate::prelude::*;
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::scheduler::schedproto::TaskAssignment;
use crate::task::{DataInfo, ErrorInfo, TaskRuntimeState};

pub struct Core {
    tasks_by_id: HashMap<TaskId, TaskRef>,
    tasks_by_key: HashMap<TaskKey, TaskRef>,
    clients: HashMap<ClientId, Client>,
    client_key_to_id: HashMap<String, ClientId>,
    workers: HashMap<WorkerId, WorkerRef>,

    update: crate::scheduler::Update,
    scheduler_sender: UnboundedSender<ToSchedulerMessage>,
    update_timeout_running: bool,

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
        let client_id = client.id();
        assert!(self.client_key_to_id.insert(client.key().to_string(), client_id).is_none());
        assert!(self.clients.insert(client_id, client).is_none());
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

    pub fn register_worker(&mut self, worker_ref: WorkerRef) {
        let worker_id = {
            let worker = worker_ref.get();
            self.update.new_workers.push(worker.make_sched_info());
            self.send_scheduler_update(false);
            worker.id()
        };
        assert!(self.workers.insert(worker_id, worker_ref).is_none());
    }

    pub fn unregister_client(&mut self, client_id: ClientId) {
        let client = self.clients.remove(&client_id).unwrap();
        assert!(self.client_key_to_id.remove(client.key()).is_some());
    }

    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        assert!(self.workers.remove(&worker_id).is_some());
    }

    // ! This function modifies update, but do not triggers, send_update
    // You have to done it manually.
    pub fn add_task(&mut self, task_ref: TaskRef) {
        let task_id = task_ref.get().id;
        let task_key = task_ref.get().key.clone();
        self.update.new_tasks.push(task_ref.get().make_sched_info());
        assert!(self.tasks_by_id.insert(task_id, task_ref.clone()).is_none());
        assert!(self.tasks_by_key.insert(task_key, task_ref).is_none());
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
        self.clients.get_mut(&id).unwrap_or_else(|| {
            panic!("Asking for invalid client id={}", id);
        })
    }

    pub fn get_client_id_by_key(&self, key: &str) -> ClientId {
        self.client_key_to_id.get(key).map(|r| *r).unwrap_or_else(|| {
            panic!("Asking for invalid client key={}", key);
        })
    }

    fn _send_scheduler_update_now(&mut self) {
        log::debug!("Sending update to scheduler");
        let update = std::mem::replace(&mut self.update, Default::default());
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
    }

    pub fn new_self_ref(&self) -> CoreRef {
        self.self_ref.clone().unwrap()
    }

    pub fn process_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        log::debug!("Assignments from scheduler: {:?}", assignments);
        let mut notifications = Notifications::new();
        for assignment in assignments {
            let worker_ref = self.workers.get(&assignment.worker).expect("Worker from assignment not found").clone();
            let task_ref = self.get_task_by_id_or_panic(assignment.task);

            let mut task = task_ref.get_mut();
            assert!(task.is_waiting());
            assert!(task.worker.is_none());
            task.worker = Some(worker_ref.clone());
            if task.is_ready() {
                task.state = TaskRuntimeState::Assigned;
                log::debug!(
                    "Task task={} scheduled & assigned to worker={}",
                    assignment.task,
                    assignment.worker
                );
                notifications.compute_task_on_worker(worker_ref, task_ref.clone());
            } else {
                task.state = TaskRuntimeState::Scheduled;
                log::debug!(
                    "Task task={} scheduled to worker={}",
                    assignment.task,
                    assignment.worker
                );
            }
        }
        notifications.send(self);
    }

    pub fn _on_task_error_helper(&mut self, task: &mut Task, task_ref: &TaskRef, error_info: Rc<ErrorInfo>, mut notifications: &mut Notifications) {
        task.state = TaskRuntimeState::Error(error_info);
        self.unregister_as_consumer(&task, task_ref, &mut notifications);
        for client_id in task.subscribed_clients() {
            notifications.notify_client_about_task_error(*client_id, task_ref.clone());
        }
    }

    pub fn on_task_error(&mut self, worker: &WorkerRef, task_key: TaskKey, error_info: ErrorInfo, mut notifications: &mut Notifications) {
        let task_ref = self.get_task_by_key_or_panic(&task_key).clone();
        let error_info = Rc::new(error_info);
        let mut task_refs = {
            let mut task = task_ref.get_mut();
            assert!(task.is_assigned());
            self._on_task_error_helper(&mut task, &task_ref, error_info.clone(), &mut notifications);
            task.collect_consumers()
        };

        for task_ref in task_refs {
            let mut task = task_ref.get_mut();
            assert!(task.is_waiting() || task.is_scheduled());
            self._on_task_error_helper(&mut task, &task_ref, error_info.clone(), &mut notifications);
        }
    }

    pub fn unregister_as_consumer(&self, task: &Task, task_ref: &TaskRef, notifications: &mut Notifications) {
        for input_id in &task.dependencies {
            let tr = self.get_task_by_id_or_panic(*input_id);
            let mut t = tr.get_mut();
            assert!(t.consumers.remove(&task_ref));
            t.check_if_data_cannot_be_removed(notifications);
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
            assert!(task.is_assigned());
            assert!(task.worker.as_ref().unwrap() == worker);
            task.state = TaskRuntimeState::Finished(DataInfo { size: msg.nbytes, r#type: msg.r#type });
            for consumer in &task.consumers {
                let mut t = consumer.get_mut();
                assert!(t.is_waiting() || t.is_scheduled());
                t.unfinished_inputs -= 1;
                if t.unfinished_inputs == 0 && t.is_scheduled() {
                    t.state = TaskRuntimeState::Assigned;
                    notifications.compute_task_on_worker(t.worker.clone().unwrap(), consumer.clone());
                }
            }
            self.unregister_as_consumer(&task, &task_ref, &mut notifications);
            self.notify_key_in_memory(&task);
            task.check_if_data_cannot_be_removed(notifications);
        }
        self.send_scheduler_update(true);
    }

    fn notify_key_in_memory(&mut self, task: &Task) {
        for client_id in task.subscribed_clients() {
            match self.clients.get_mut(client_id) {
                Some(client) => {
                    client.send_message(ToClientMessage::KeyInMemory(KeyInMemoryMsg {
                        key: task.key.clone(),
                        r#type: task.data_info().unwrap().r#type.clone(),
                    }));
                }
                None => {
                    panic!("Task id={} finished for a dropped client={}", task.id, client_id);
                }
            }
        }
    }
}

impl CoreRef {
    pub fn new(scheduler_sender: UnboundedSender<ToSchedulerMessage>) -> Self {
        let core_ref = Self::wrap(Core {
            tasks_by_id: Default::default(),
            tasks_by_key: Default::default(),

            id_counter: 0,

            workers: Default::default(),
            clients: Default::default(),
            client_key_to_id: Default::default(),

            scheduler_sender,
            update: Default::default(),
            update_timeout_running: false,

            uid: "123_TODO".into(),
            self_ref: None,
        });
        {
            let mut core = core_ref.get_mut();
            core.self_ref = Some(core_ref.clone());

            // Prepare default guess of net bandwidth [MB/s], TODO: better guess,
            // It will be send with the first update message.
            core.update.network_bandwidth = Some(100.0);
        }
        core_ref
    }

    pub async fn observe_scheduler(
        self,
        mut recv: UnboundedReceiver<FromSchedulerMessage>,
    ) {
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
