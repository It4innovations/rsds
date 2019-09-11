use crate::common::WrappedRcRefCell;
use crate::prelude::*;
use crate::scheduler::schedproto::TaskUpdate;
use crate::scheduler::{FromSchedulerMessage, Scheduler, ToSchedulerMessage};
use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::runtime::current_thread;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct Core {
    tasks_by_id: HashMap<TaskId, Rc<Task>>,
    tasks_by_key: HashMap<TaskKey, Rc<Task>>,
    clients: HashMap<ClientId, Client>,
    workers: HashMap<WorkerId, WorkerRef>,

    task_state_changed: HashSet<TaskRef>,
    update: crate::scheduler::Update,
    scheduler_sender: UnboundedSender<ToSchedulerMessage>,
    update_timeout_running: bool,

    id_counter: u64,
    uid: String,

    // This is reference to itself
    // For real cleanup you have to remove this cycle
    // However, since Core pracitcally global object, it is never done
    self_ref: Option<CoreRef>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl Core {
    pub fn register_client(&mut self, client: Client) {
        assert!(self.clients.insert(client.id(), client).is_none());
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
            worker.id()
        };
        assert!(self.workers.insert(worker_id, worker_ref).is_none());
    }

    pub fn unregister_client(&mut self, client_id: ClientId) {
        assert!(self.clients.remove(&client_id).is_some());
    }

    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        assert!(self.workers.remove(&worker_id).is_some());
    }

    // ! This function modifies update, but do not tiggers, send_update
    // You have to done it manually.
    pub fn set_task_state_changed(&mut self, task_rc: Rc<Task>) {
        self.task_state_changed.insert(TaskRef::new(task_rc));
    }

    // ! This function modifies update, but do not tiggers, send_update
    // You have to done it manually.
    pub fn add_task(&mut self, task_ref: Rc<Task>) {
        self.update.new_tasks.push(task_ref.make_sched_info());
        assert!(self
            .tasks_by_id
            .insert(task_ref.id, task_ref.clone())
            .is_none());
        assert!(self
            .tasks_by_key
            .insert(task_ref.key.clone(), task_ref)
            .is_none());
    }

    pub fn get_task_by_key_or_panic(&mut self, key: &str) -> &mut Rc<Task> {
        self.tasks_by_key.get_mut(key).unwrap()
    }

    pub fn get_task_by_id_or_panic(&mut self, id: TaskId) -> &mut Rc<Task> {
        self.tasks_by_id.get_mut(&id).unwrap()
    }

    pub fn send_update(&mut self) {
        if self.update_timeout_running {
            return;
        }
        self.update_timeout_running = true;
        let core_ref = self.new_self_ref();
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(50))
            .unwrap();
        current_thread::spawn(tokio::timer::delay(deadline).map(move |()| {
            log::debug!("Sending update to scheduler");
            let mut core = core_ref.get_mut();
            core.update_timeout_running = false;
            let mut update = std::mem::replace(&mut core.update, Default::default());
            update.task_updates = core
                .task_state_changed
                .iter()
                .filter_map(|r| r.get().make_sched_update())
                .collect();
            core.task_state_changed.clear();
            let msg = ToSchedulerMessage::Update(update);
            core.scheduler_sender.try_send(msg).unwrap();
        }));
    }

    pub fn new_self_ref(&self) -> CoreRef {
        self.self_ref.clone().unwrap()
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

            scheduler_sender,
            task_state_changed: Default::default(),
            update: Default::default(),
            update_timeout_running: false,

            uid: "123_TODO".into(),
            self_ref: None,
        });
        {
            let mut core = core_ref.get_mut();
            core.self_ref = Some(core_ref.clone());

            // Prepare default guess of netbandwidth [MB/s], TODO: better guess,
            // It will be send with the first update message.
            core.update.network_bandwidth = Some(100.0);
        }
        core_ref
    }

    pub async fn observe_scheduler(
        self,
        mut recv: UnboundedReceiver<FromSchedulerMessage>,
    ) -> crate::Result<()> {
        log::debug!("Starting scheduler");

        let first_message = recv.next().await;
        match first_message {
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
                    dbg!("Task assignments {}", assignments);
                }
                FromSchedulerMessage::Register(_) => {
                    panic!("Double registration of scheduler");
                }
            }
        }
        Ok(())
    }

    pub fn uid(&self) -> String {
        self.get().uid.clone()
    }
}
