use super::task::{SchedulerTaskState, Task, TaskRef};
use super::utils::compute_b_level;
use super::worker::{Worker, WorkerRef};
use crate::common::{Map, Set};
use crate::scheduler::comm::SchedulerComm;
use crate::scheduler::schedproto::{
    SchedulerRegistration, TaskAssignment, TaskId, TaskStealResponse, TaskUpdate, TaskUpdateType,
    WorkerId,
};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use futures::StreamExt;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::time::Duration;

pub struct Scheduler {
    network_bandwidth: f32,
    workers: Map<WorkerId, WorkerRef>,
    tasks: Map<TaskId, TaskRef>,
    ready_to_assign: Vec<TaskRef>,
    new_tasks: Vec<TaskRef>,
    rng: ThreadRng,
}

type Notifications = Set<TaskRef>;

const MIN_SCHEDULING_DELAY: Duration = Duration::from_millis(15);

impl Scheduler {
    pub fn new() -> Self {
        Self {
            workers: Default::default(),
            tasks: Default::default(),
            ready_to_assign: Default::default(),
            new_tasks: Default::default(),
            network_bandwidth: 100.0, // Guess better default
            rng: thread_rng(),
        }
    }

    fn get_task(&self, task_id: TaskId) -> &TaskRef {
        self.tasks
            .get(&task_id)
            .unwrap_or_else(|| panic!("Task {} not found", task_id))
    }

    fn get_worker(&self, worker_id: WorkerId) -> &WorkerRef {
        self.workers
            .get(&worker_id)
            .unwrap_or_else(|| panic!("Worker {} not found", worker_id))
    }

    fn send_notifications(&self, notifications: Notifications, sender: &mut SchedulerComm) {
        let assignments: Vec<_> = notifications
            .into_iter()
            .map(|tr| {
                let task = tr.get();
                let worker_ref = task.assigned_worker.clone().unwrap();
                let worker = worker_ref.get();
                TaskAssignment {
                    task: task.id,
                    worker: worker.id,
                    priority: 0,
                    // TODO derive priority from b-level
                }
            })
            .collect();
        sender.send(FromSchedulerMessage::TaskAssignments(assignments));
    }

    pub async fn start(mut self, mut comm: SchedulerComm) -> crate::Result<()> {
        log::debug!("Workstealing scheduler initialized");

        comm.send(FromSchedulerMessage::Register(SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "workstealing-scheduler".into(),
            scheduler_version: "0.0".into(),
        }));

        while let Some(msgs) = comm.recv.next().await {
            /* TODO: Add delay that prevents calling scheduler too often */
            if self.update(msgs) {
                let mut notifications = Notifications::new();
                self.schedule(&mut notifications);
                self.send_notifications(notifications, &mut comm);
            }
        }
        log::debug!("Scheduler closed");
        Ok(())
    }

    fn assign_task_to_worker(
        &mut self,
        task: &mut Task,
        task_ref: TaskRef,
        worker: &mut Worker,
        worker_ref: WorkerRef,
        notifications: &mut Notifications,
    ) {
        notifications.insert(task_ref.clone());
        if let Some(wr) = &task.assigned_worker {
            assert!(!wr.eq(&worker_ref));
            let mut worker = wr.get_mut();
            assert!(worker.tasks.remove(&task_ref));
        }
        task.assigned_worker = Some(worker_ref);
        assert!(worker.tasks.insert(task_ref));
    }

    pub fn schedule(&mut self, mut notifications: &mut Notifications) {
        log::debug!("Scheduling started");
        if self.workers.is_empty() {
            return;
        }
        if !self.new_tasks.is_empty() {
            // TODO: utilize information and do not recompute all b-levels
            compute_b_level(&self.tasks);
            self.new_tasks = Vec::new()
        }

        for tr in std::mem::take(&mut self.ready_to_assign).into_iter() {
            let mut task = tr.get_mut();
            let worker_ref = self.choose_worker_for_task(&task);
            let mut worker = worker_ref.get_mut();
            log::debug!("Task {} initially assigned to {}", task.id, worker.id);
            assert!(task.assigned_worker.is_none());
            self.assign_task_to_worker(
                &mut task,
                tr.clone(),
                &mut worker,
                worker_ref.clone(),
                &mut notifications,
            );
        }

        let mut balanced_tasks = Vec::new();
        let has_underload_workers = self.workers.values().any(|wr| {
            let worker = wr.get();
            worker.is_underloaded()
        });

        if !has_underload_workers {
            return; // Terminate as soon possible when there is nothing to balance
        }

        log::debug!("Balancing started");

        for wr in self.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len > worker.ncpus {
                log::debug!("Worker {} offers {} tasks", worker.id, len);
                for tr in &worker.tasks {
                    tr.get_mut().take_flag = false;
                }
                balanced_tasks.extend(worker.tasks.iter().filter(|tr| !tr.get().pinned).cloned());
            }
        }

        let mut underload_workers = Vec::new();
        for wr in self.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len < worker.ncpus {
                log::debug!("Worker {} is underloaded ({} tasks)", worker.id, len);
                let mut ts = balanced_tasks.clone();
                ts.sort_by_cached_key(|tr| std::u64::MAX - task_transfer_cost(&tr.get(), &wr));
                underload_workers.push((wr.clone(), ts));
            }
        }
        underload_workers.sort_by_key(|x| x.0.get().tasks.len());

        let mut n_tasks = underload_workers[0].0.get().tasks.len();
        loop {
            let mut change = false;
            for (wr, ts) in underload_workers.iter_mut() {
                let mut worker = wr.get_mut();
                if worker.tasks.len() > n_tasks {
                    break;
                }
                if ts.is_empty() {
                    continue;
                }
                while let Some(tr) = ts.pop() {
                    let mut task = tr.get_mut();
                    if task.take_flag {
                        continue;
                    }
                    task.take_flag = true;
                    let wid = {
                        let wr2 = task.assigned_worker.clone().unwrap();
                        let worker2 = wr2.get();
                        if worker2.tasks.len() <= n_tasks {
                            continue;
                        }
                        worker2.id
                    };
                    log::debug!(
                        "Changing assignment of task={} from worker={} to worker={}",
                        task.id,
                        wid,
                        worker.id
                    );
                    self.assign_task_to_worker(
                        &mut task,
                        tr.clone(),
                        &mut worker,
                        wr.clone(),
                        &mut notifications,
                    );
                    break;
                }
                change = true;
            }
            if !change {
                break;
            }
            n_tasks += 1;
        }
        log::debug!("Balancing finished");
    }

    fn task_update(&mut self, tu: TaskUpdate) -> bool {
        let tref = self.get_task(tu.id).clone();
        let mut task = tref.get_mut();
        match tu.state {
            TaskUpdateType::Finished => {
                log::debug!("Task id={} is finished on worker={}", task.id, tu.worker);
                let worker = self.get_worker(tu.worker).clone();
                assert!(task.is_waiting() && task.is_ready());
                task.state = SchedulerTaskState::Finished;
                task.size = tu.size.unwrap();
                let wr = task.assigned_worker.take().unwrap();
                let mut invoke_scheduling = {
                    let mut worker = wr.get_mut();
                    assert!(worker.tasks.remove(&tref));
                    worker.is_underloaded()
                };
                for tref in &task.consumers {
                    let mut t = tref.get_mut();
                    if t.unfinished_deps <= 1 {
                        assert!(t.unfinished_deps > 0);
                        assert!(t.is_waiting());
                        t.unfinished_deps -= 1;
                        log::debug!("Task {} is ready", t.id);
                        self.ready_to_assign.push(tref.clone());
                        invoke_scheduling = true;
                    } else {
                        t.unfinished_deps -= 1;
                    }
                }
                task.placement.insert(worker);
                return invoke_scheduling;
            }
            TaskUpdateType::Placed => {
                let worker = self.get_worker(tu.worker).clone();
                assert!(task.is_finished());
                task.placement.insert(worker);
            }
            TaskUpdateType::Removed => {
                let worker = self.get_worker(tu.worker);
                //task.placement.remove(worker);
                if !task.placement.remove(worker) {
                    panic!(
                        "Worker {} removes task {}, but it was not there.",
                        worker.get().id,
                        task.id
                    );
                }
                /*let index = task.placement.iter().position(|x| x == worker).unwrap();
                task.placement.remove(index);*/
            }
        }
        false
    }

    fn rollback_steal(&mut self, response: TaskStealResponse) -> bool {
        let tref = self.get_task(response.id);
        let mut task = tref.get_mut();
        let new_wref = self.get_worker(response.to_worker);

        let need_balancing = {
            let wref = task.assigned_worker.as_ref().unwrap();
            if wref == new_wref {
                return false;
            }
            let mut worker = wref.get_mut();
            worker.tasks.remove(&tref);
            worker.is_underloaded()
        };
        task.pinned = true;
        task.assigned_worker = Some(new_wref.clone());
        let mut new_worker = new_wref.get_mut();
        new_worker.tasks.insert(tref.clone());
        need_balancing
    }

    pub fn update(&mut self, messages: Vec<ToSchedulerMessage>) -> bool {
        let mut invoke_scheduling = false;
        for message in messages {
            match message {
                ToSchedulerMessage::TaskUpdate(tu) => {
                    invoke_scheduling |= self.task_update(tu);
                }
                ToSchedulerMessage::TaskStealResponse(sr) => {
                    if !sr.success {
                        invoke_scheduling |= self.rollback_steal(sr);
                    }
                }
                ToSchedulerMessage::NewTask(ti) => {
                    log::debug!("New task {} #inputs={}", ti.id, ti.inputs.len());
                    let task_id = ti.id;
                    let inputs: Vec<_> = ti
                        .inputs
                        .iter()
                        .map(|id| self.tasks.get(id).unwrap().clone())
                        .collect();
                    let task = TaskRef::new(ti, inputs);
                    if task.get().is_ready() {
                        log::debug!("Task {} is ready", task_id);
                        self.ready_to_assign.push(task.clone());
                    }
                    self.new_tasks.push(task.clone());
                    assert!(self.tasks.insert(task_id, task).is_none());
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::RemoveTask(task_id) => {
                    log::debug!("Remove task {}", task_id);
                    let tref = self.get_task(task_id).clone();
                    let task = tref.get();
                    assert!(task.is_finished()); // TODO: Define semantics of removing non-finished tasks
                    assert!(self.tasks.remove(&task_id).is_some());
                }
                ToSchedulerMessage::NewFinishedTask(ti) => {
                    let placement: Set<WorkerRef> = ti
                        .workers
                        .iter()
                        .map(|id| self.get_worker(*id).clone())
                        .collect();
                    let task_id = ti.id;
                    let task = TaskRef::new_finished(ti, placement);
                    assert!(self.tasks.insert(task_id, task).is_none());
                }
                ToSchedulerMessage::NewWorker(wi) => {
                    assert!(self.workers.insert(wi.id, WorkerRef::new(wi),).is_none());
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::NetworkBandwidth(nb) => {
                    self.network_bandwidth = nb;
                }
            }
        }
        invoke_scheduling

        // HACK, random scheduler
        /*if !self.workers.is_empty() {
            use rand::seq::SliceRandom;
            let mut result = Vec::new();
            let mut rng = rand::thread_rng();
            let ws: Vec<WorkerId> = self.workers.values().map(|w| w.get().id).collect();
            // TMP HACK
            for task_id in &self._tmp_hack {
                result.push(TaskAssignment {
                    task: *task_id,
                    worker: *ws.choose(&mut rng).unwrap(),
                    priority: 0,
                });
            }
            self._tmp_hack.clear();

            sender
                .send(FromSchedulerMessage::TaskAssignments(result))
                .unwrap();
        }*/
    }

    fn choose_worker_for_task(&mut self, task: &Task) -> WorkerRef {
        let mut costs = std::u64::MAX;
        let mut workers = Vec::new();
        for wr in self.workers.values() {
            let c = task_transfer_cost(task, wr);
            if c < costs {
                costs = c;
                workers.clear();
                workers.push(wr.clone());
            } else if c == costs {
                workers.push(wr.clone());
            }
        }
        if workers.len() == 1 {
            workers.pop().unwrap()
        } else {
            workers.choose(&mut self.rng).unwrap().clone()
        }
    }

    pub fn sanity_check(&self) {
        for (id, tr) in &self.tasks {
            let task = tr.get();
            assert_eq!(task.id, *id);
            task.sanity_check(&tr);
            if let Some(w) = &task.assigned_worker {
                assert!(self.workers.contains_key(&w.get().id));
            }
        }

        for wr in self.workers.values() {
            let worker = wr.get();
            worker.sanity_check(&wr);
        }
    }
}

fn task_transfer_cost(task: &Task, worker_ref: &WorkerRef) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    task.inputs
        .iter()
        .take(512)
        .map(|tr| {
            let t = tr.get();
            if t.placement.contains(worker_ref) {
                0u64
            } else {
                t.size
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::schedproto::{TaskInfo, WorkerInfo};

    fn init() {
        //std::env::set_var("RUST_LOG", "debug");
        //pretty_env_logger::init();
        let _ = pretty_env_logger::try_init().map_err(|_| {
            println!("Logging initialized failed");
        });
    }

    fn new_task(id: TaskId, inputs: Vec<TaskId>) -> ToSchedulerMessage {
        ToSchedulerMessage::NewTask(TaskInfo { id, inputs })
    }

    /* Graph simple
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5

    */
    fn submit_graph_simple(scheduler: &mut Scheduler) {
        scheduler.update(vec![
            new_task(1, vec![]),
            new_task(2, vec![1]),
            new_task(3, vec![1]),
            new_task(4, vec![2, 3]),
            new_task(5, vec![4]),
            new_task(6, vec![3]),
            new_task(7, vec![6]),
        ]);
    }

    /* Graph reduce

        0   1   2   3   4 .. n-1
         \  \   |   /  /    /
          \--\--|--/--/----/
                |
                n
    */
    fn submit_graph_reduce(scheduler: &mut Scheduler, size: usize) {
        let mut tasks: Vec<_> = (0..size)
            .map(|t| new_task(t as TaskId, Vec::new()))
            .collect();
        tasks.push(new_task(size as TaskId, (0..size as TaskId).collect()));
        scheduler.update(tasks);
    }

    /* Graph split

        0
        |\---\---\------\
        | \   \   \     \
        1  2   3   4  .. n-1
    */
    fn submit_graph_split(scheduler: &mut Scheduler, size: usize) {
        let mut tasks = vec![new_task(0, Vec::new())];
        tasks.extend((1..=size).map(|t| new_task(t as TaskId, vec![0])));
        scheduler.update(tasks);
    }

    fn connect_workers(scheduler: &mut Scheduler, count: u32, n_cpus: u32) {
        for i in 0..count {
            scheduler.update(vec![ToSchedulerMessage::NewWorker(WorkerInfo {
                id: 100 + i as WorkerId,
                n_cpus,
            })]);
        }
    }

    fn finish_task(scheduler: &mut Scheduler, task_id: TaskId, worker_id: WorkerId, size: u64) {
        scheduler.update(vec![ToSchedulerMessage::TaskUpdate(TaskUpdate {
            state: TaskUpdateType::Finished,
            id: task_id,
            worker: worker_id,
            size: Some(size),
        })]);
    }

    fn run_schedule(scheduler: &mut Scheduler) -> Set<TaskId> {
        let mut notifications = Notifications::new();
        scheduler.schedule(&mut notifications);
        notifications.iter().map(|tr| tr.get().id).collect()
    }

    fn assigned_worker(scheduler: &mut Scheduler, task_id: TaskId) -> WorkerId {
        scheduler
            .tasks
            .get(&task_id)
            .expect("Unknown task")
            .get()
            .assigned_worker
            .as_ref()
            .expect("Worker not assigned")
            .get()
            .id
    }

    #[test]
    fn test_b_level() {
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        assert_eq!(scheduler.ready_to_assign.len(), 1);
        assert_eq!(scheduler.ready_to_assign[0].get().id, 1);
        connect_workers(&mut scheduler, 1, 1);
        scheduler.schedule(&mut Notifications::new());
        assert_eq!(scheduler.get_task(7).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(6).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(5).get().b_level, 1.0);
        assert_eq!(scheduler.get_task(4).get().b_level, 2.0);
        assert_eq!(scheduler.get_task(3).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(2).get().b_level, 3.0);
        assert_eq!(scheduler.get_task(1).get().b_level, 4.0);
    }

    #[test]
    fn test_simple_w1_1() {
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 1, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&mut scheduler, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.tasks.get(&2).unwrap();
        let _t3 = scheduler.tasks.get(&3).unwrap();
        assert_eq!(assigned_worker(&mut scheduler, 2), 100);
        assert_eq!(assigned_worker(&mut scheduler, 3), 100);
        scheduler.sanity_check();
    }

    #[test]
    fn test_simple_w2_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 2, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&mut scheduler, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.tasks.get(&2).unwrap();
        let _t3 = scheduler.tasks.get(&3).unwrap();
        assert_ne!(
            assigned_worker(&mut scheduler, 2),
            assigned_worker(&mut scheduler, 3)
        );
        scheduler.sanity_check();
    }

    #[test]
    fn test_reduce_w5_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_reduce(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 0..5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&mut scheduler, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_split_w5_1() {
        init();
        let mut scheduler = Scheduler::new();
        submit_graph_split(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let _n = run_schedule(&mut scheduler);
        let w = assigned_worker(&mut scheduler, 0);
        finish_task(&mut scheduler, 0, w, 100_000_000);
        scheduler.sanity_check();

        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 1..=5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&mut scheduler, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_consecutive_submit() {
        init();
        let mut scheduler = Scheduler::new();
        connect_workers(&mut scheduler, 5, 1);
        scheduler.update(vec![new_task(1, vec![])]);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();
        finish_task(&mut scheduler, 1, 101, 1000_1000);
        scheduler.sanity_check();
        scheduler.update(vec![new_task(2, vec![1])]);
        let n = run_schedule(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&2));
        scheduler.sanity_check();
    }
}
