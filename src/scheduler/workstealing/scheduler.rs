use crate::common::Map;
use crate::scheduler::graph::{assign_task_to_worker, Notifications, SchedulerGraph};
use crate::scheduler::protocol::{
    SchedulerRegistration, TaskStealResponse, TaskUpdate, TaskUpdateType,
};
use crate::scheduler::task::Task;
use crate::scheduler::utils::{compute_b_level, task_transfer_cost};
use crate::scheduler::worker::WorkerRef;
use crate::scheduler::{Scheduler, SchedulerSender, ToSchedulerMessage, WorkerId};
use rand::prelude::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

#[derive(Debug)]
pub struct WorkstealingScheduler {
    graph: SchedulerGraph,
    /// Notifications are cached here to avoid reallocating them on every update
    notifications: Notifications,
    random: SmallRng,
}

impl WorkstealingScheduler {
    pub fn new() -> Self {
        Self {
            graph: SchedulerGraph::default(),
            notifications: Default::default(),
            random: SmallRng::from_entropy(),
        }
    }

    fn handle_messages(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut SchedulerSender) {
        self.notifications.clear();

        trace_time!("scheduler", "update", {
            if self.update(messages) {
                trace_time!("scheduler", "schedule", {
                    if self.schedule() {
                        trace_time!("scheduler", "balance", self.balance());
                    }
                });

                self.graph.send_notifications(&self.notifications, sender);
            }
        });
    }

    /// Returns true if balancing is needed.
    pub fn schedule(&mut self) -> bool {
        if self.graph.workers.is_empty() {
            return false;
        }

        log::debug!("Scheduling started");
        if !self.graph.new_tasks.is_empty() {
            // TODO: utilize information and do not recompute all b-levels
            trace_time!("scheduler", "blevel", {
                compute_b_level(&self.graph.tasks);
            });

            self.graph.new_tasks.clear();
        }

        for tr in self.graph.ready_to_assign.drain(..) {
            let mut task = tr.get_mut();
            let worker_ref = choose_worker_for_task(&task, &self.graph.workers, &mut self.random);
            let mut worker = worker_ref.get_mut();
            log::debug!("Task {} initially assigned to {}", task.id, worker.id);
            assert!(task.assigned_worker.is_none());
            assign_task_to_worker(
                &mut task,
                tr.clone(),
                &mut worker,
                worker_ref.clone(),
                &mut self.notifications,
            );
        }

        let has_underload_workers = self.graph.workers.values().any(|wr| {
            let worker = wr.get();
            worker.is_underloaded()
        });

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    fn balance(&mut self) {
        log::debug!("Balancing started");
        let mut balanced_tasks = Vec::new();
        for wr in self.graph.workers.values() {
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
        for wr in self.graph.workers.values() {
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
                    assign_task_to_worker(
                        &mut task,
                        tr.clone(),
                        &mut worker,
                        wr.clone(),
                        &mut self.notifications,
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
        match tu.state {
            TaskUpdateType::Finished => {
                let (mut invoke_scheduling, worker) =
                    self.graph.finish_task(tu.id, tu.worker, tu.size.unwrap());
                invoke_scheduling |= worker.get().is_underloaded();
                return invoke_scheduling;
            }
            TaskUpdateType::Placed => self.graph.place_task_on_worker(tu.id, tu.worker),
            TaskUpdateType::Removed => self.graph.remove_task_from_worker(tu.id, tu.worker),
        }
        false
    }

    fn rollback_steal(&mut self, response: TaskStealResponse) -> bool {
        let tref = self.graph.get_task(response.id);
        let mut task = tref.get_mut();
        let new_wref = self.graph.get_worker(response.to_worker);

        let need_balancing = {
            let wref = task.assigned_worker.as_ref().unwrap();
            if wref == new_wref {
                return false;
            }
            for tref in &task.inputs {
                let mut t = tref.get_mut();
                t.remove_future_placement(&wref);
                t.set_future_placement(new_wref.clone());
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
                    self.graph.add_task(ti);
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::RemoveTask(task_id) => self.graph.remove_task(task_id),
                ToSchedulerMessage::NewFinishedTask(ti) => self.graph.add_finished_task(ti),
                ToSchedulerMessage::NewWorker(wi) => {
                    self.graph.add_worker(wi);
                    invoke_scheduling = true;
                }
                ToSchedulerMessage::NetworkBandwidth(nb) => {
                    self.graph.network_bandwidth = nb;
                }
            }
        }
        invoke_scheduling
    }

    pub fn sanity_check(&self) {
        self.graph.sanity_check()
    }
}

impl Scheduler for WorkstealingScheduler {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "workstealing-scheduler".into(),
            scheduler_version: "0.0".into(),
        }
    }

    #[inline]
    fn update(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut SchedulerSender) {
        self.handle_messages(messages, sender)
    }
}

fn choose_worker_for_task(
    task: &Task,
    worker_map: &Map<WorkerId, WorkerRef>,
    random: &mut SmallRng,
) -> WorkerRef {
    let mut costs = std::u64::MAX;
    let mut workers = Vec::new();
    for wr in worker_map.values() {
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
        workers.choose(random).unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Map, Set};
    use crate::scheduler::protocol::{TaskInfo, WorkerInfo};
    use crate::scheduler::{TaskId, WorkerId};

    fn init() {
        let _ = env_logger::try_init().map_err(|_| {
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
    fn submit_graph_simple(scheduler: &mut WorkstealingScheduler) {
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
    fn submit_graph_reduce(scheduler: &mut WorkstealingScheduler, size: usize) {
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
    fn submit_graph_split(scheduler: &mut WorkstealingScheduler, size: usize) {
        let mut tasks = vec![new_task(0, Vec::new())];
        tasks.extend((1..=size).map(|t| new_task(t as TaskId, vec![0])));
        scheduler.update(tasks);
    }

    fn connect_workers(scheduler: &mut WorkstealingScheduler, count: u32, n_cpus: u32) {
        for i in 0..count {
            scheduler.update(vec![ToSchedulerMessage::NewWorker(WorkerInfo {
                id: 100 + i as WorkerId,
                n_cpus,
                hostname: "worker".into(),
            })]);
        }
    }

    fn finish_task(
        scheduler: &mut WorkstealingScheduler,
        task_id: TaskId,
        worker_id: WorkerId,
        size: u64,
    ) {
        scheduler.update(vec![ToSchedulerMessage::TaskUpdate(TaskUpdate {
            state: TaskUpdateType::Finished,
            id: task_id,
            worker: worker_id,
            size: Some(size),
        })]);
    }

    fn run_schedule(scheduler: &mut WorkstealingScheduler) -> Set<TaskId> {
        scheduler.notifications.clear();
        if scheduler.schedule() {
            scheduler.balance();
        }
        scheduler.notifications.iter().map(|tr| tr.get().id).collect()
    }

    fn assigned_worker(scheduler: &mut WorkstealingScheduler, task_id: TaskId) -> WorkerId {
        scheduler
            .graph
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
        let mut scheduler = WorkstealingScheduler::new();
        submit_graph_simple(&mut scheduler);
        assert_eq!(scheduler.graph.ready_to_assign.len(), 1);
        assert_eq!(scheduler.graph.ready_to_assign[0].get().id, 1);
        connect_workers(&mut scheduler, 1, 1);
        run_schedule(&mut scheduler);
        assert_eq!(scheduler.graph.get_task(7).get().b_level, 1);
        assert_eq!(scheduler.graph.get_task(6).get().b_level, 2);
        assert_eq!(scheduler.graph.get_task(5).get().b_level, 1);
        assert_eq!(scheduler.graph.get_task(4).get().b_level, 2);
        assert_eq!(scheduler.graph.get_task(3).get().b_level, 3);
        assert_eq!(scheduler.graph.get_task(2).get().b_level, 3);
        assert_eq!(scheduler.graph.get_task(1).get().b_level, 4);
    }

    #[test]
    fn test_simple_w1_1() {
        let mut scheduler = WorkstealingScheduler::new();
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
        let _t2 = scheduler.graph.tasks.get(&2).unwrap();
        let _t3 = scheduler.graph.tasks.get(&3).unwrap();
        assert_eq!(assigned_worker(&mut scheduler, 2), 100);
        assert_eq!(assigned_worker(&mut scheduler, 3), 100);
        scheduler.sanity_check();
    }

    #[test]
    fn test_simple_w2_1() {
        init();
        let mut scheduler = WorkstealingScheduler::new();
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
        let _t2 = scheduler.graph.tasks.get(&2).unwrap();
        let _t3 = scheduler.graph.tasks.get(&3).unwrap();
        assert_ne!(
            assigned_worker(&mut scheduler, 2),
            assigned_worker(&mut scheduler, 3)
        );
        scheduler.sanity_check();
    }

    #[test]
    fn test_reduce_w5_1() {
        init();
        let mut scheduler = WorkstealingScheduler::new();
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
        let mut scheduler = WorkstealingScheduler::new();
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
        let mut scheduler = WorkstealingScheduler::new();
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
