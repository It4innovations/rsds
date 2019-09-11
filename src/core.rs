use crate::common::WrappedRcRefCell;
use crate::prelude::*;
use crate::scheduler::schedproto::TaskAssignment;
use crate::scheduler::schedproto::TaskUpdate;
use crate::task::TaskRuntimeState;
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::runtime::current_thread;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use crate::messages::workermsg::{ToWorkerMessage, TaskFinishedMsg};
use crate::worker::send_tasks_to_workers;

pub struct Core {
    tasks_by_id: HashMap<TaskId, TaskRef>,
    tasks_by_key: HashMap<TaskKey, TaskRef>,
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
    // However, since Core practically global object, it is never done
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
            self.send_scheduler_update();
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

    // ! This function modifies update, but do not triggers, send_update
    // You have to done it manually.
    pub fn set_task_state_changed(&mut self, task_ref: TaskRef) {
        self.task_state_changed.insert(task_ref);
    }

    // ! This function modifies update, but do not triggers, send_update
    // You have to done it manually.
    pub fn add_task(&mut self, task_ref: TaskRef) {
        let task_id = task_ref.get().id;
        let task_key = task_ref.get().key.clone();
        self.update.new_tasks.push(task_ref.get().make_sched_info());
        assert!(self
            .tasks_by_id
            .insert(task_id, task_ref.clone())
            .is_none());
        assert!(self
            .tasks_by_key
            .insert(task_key, task_ref)
            .is_none());
    }

    pub fn get_task_by_key_or_panic(&self, key: &str) -> &TaskRef {
        self.tasks_by_key.get(key).unwrap()
    }

    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks_by_id.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    pub fn send_scheduler_update(&mut self) {
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

    pub fn process_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        log::debug!("Assignments from scheduler: {:?}", assignments);
        let mut tasks_per_worker = HashMap::new();
        for assignment in assignments {
            let worker_ref = self.workers.get(&assignment.worker).unwrap().clone();
            let task_ref = self.get_task_by_id_or_panic(assignment.task);

            let mut task = task_ref.get_mut();
            assert_eq!(task.state, TaskRuntimeState::Waiting);
            assert!(task.worker.is_none());
            task.worker = Some(worker_ref.clone());
            if task.is_ready() {
                task.state = TaskRuntimeState::Assigned;
                log::debug!("Task task={} scheduled & assigned to worker={}", assignment.task, assignment.worker);
                let v = tasks_per_worker.entry(worker_ref).or_insert_with(Vec::new);
                v.push(task_ref.clone());

            } else {
                task.state = TaskRuntimeState::Scheduled;
                log::debug!("Task task={} scheduled to worker={}", assignment.task, assignment.worker);
            }
        }
        send_tasks_to_workers(self, tasks_per_worker);
    }

    pub fn on_task_finished(&mut self, worker: &WorkerRef, msg: TaskFinishedMsg, new_ready_scheduled: &mut Vec<TaskRef>) {
        let mut task_ref = self.get_task_by_key_or_panic(&msg.key);
        {
            let mut task = task_ref.get_mut();
            log::debug!("Task id={} finished on worker={}", task.id, worker.get().id());
            assert!(task.state == TaskRuntimeState::Assigned);
            assert!(task.worker.as_ref().unwrap() == worker);
            task.state = TaskRuntimeState::Finished;
            task.size = Some(msg.nbytes);
            for consumer in &task.consumers {
                let is_prepared = {
                    let mut t = consumer.get_mut();
                    assert!(t.state != TaskRuntimeState::Finished);
                    t.unfinished_inputs -= 1;
                    t.unfinished_inputs == 0 && t.state == TaskRuntimeState::Scheduled
                };
                if is_prepared {
                    new_ready_scheduled.push(consumer.clone());
                }
            }
        }
        self.set_task_state_changed(task_ref.clone());
        self.send_scheduler_update();
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

            // Prepare default guess of net bandwidth [MB/s], TODO: better guess,
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
        Ok(())
    }

    pub fn uid(&self) -> String {
        self.get().uid.clone()
    }
}
