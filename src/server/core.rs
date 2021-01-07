use crate::common::{IdCounter, Map, Set, WrappedRcRefCell};
use crate::scheduler::{TaskAssignment, TaskId, WorkerId};
use crate::server::gateway::Gateway;
use crate::server::notifications::{ErrorNotification, Notifications};
use crate::server::protocol::messages::worker::ToWorkerMessage;
use crate::server::protocol::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg,
};
use crate::server::task::{DataInfo, ErrorInfo, Task, TaskRef, TaskRuntimeState};
use crate::server::worker::WorkerRef;
use crate::trace::{
    trace_task_assign, trace_task_finish, trace_task_new, trace_task_place, trace_task_remove,
    trace_worker_new, trace_worker_steal, trace_worker_steal_response,
    trace_worker_steal_response_missing,
};

pub struct Core {
    tasks: Map<TaskId, TaskRef>,
    workers: Map<WorkerId, WorkerRef>,

    task_id_counter: IdCounter,
    worker_id_counter: IdCounter,

    scatter_counter: usize,

    pub gateway: Box<dyn Gateway>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl CoreRef {
    pub fn new(gateway: Box<dyn Gateway>) -> Self {
        CoreRef::wrap(Core {
            gateway,
            tasks: Default::default(),
            task_id_counter: Default::default(),
            worker_id_counter: Default::default(),

            workers: Default::default(),

            scatter_counter: 0,
        })
    }
}

impl Core {
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
        {
            let worker = worker_ref.get();
            trace_worker_new(worker.id, worker.ncpus, worker.address());
        }
        let worker_id = worker_ref.get().id();

        for w_ref in self.workers.values() {
            let w = w_ref.get();
            w.send_message(ToWorkerMessage::NewWorker(NewWorkerMsg {
                worker_id,
                address: worker_ref.get().listen_address.clone(),
            }));
        }
        self.workers.insert(worker_id, worker_ref);
    }

    #[inline]
    pub fn unregister_worker(&mut self, worker_id: WorkerId) {
        // TODO: send to scheduler
        assert!(self.workers.remove(&worker_id).is_some());
    }

    #[inline]
    pub fn get_and_move_scatter_counter(&mut self, size: usize) -> usize {
        let c = self.scatter_counter;
        self.scatter_counter += size;
        c
    }

    #[inline]
    pub fn gateway(&self) -> &dyn Gateway {
        self.gateway.as_ref()
    }

    #[inline]
    pub fn get_worker_by_address(&self, address: &str) -> Option<&WorkerRef> {
        self.workers.values().find(|w| w.get().address() == address)
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let task_id = task_ref.get().id();
        assert!(self.tasks.insert(task_id, task_ref).is_none());
    }

    #[must_use]
    pub fn remove_task(&mut self, task: &mut Task) -> TaskRuntimeState {
        trace_task_remove(task.id);
        assert!(!task.has_consumers());
        assert!(self.tasks.remove(&task.id).is_some());
        std::mem::replace(&mut task.state, TaskRuntimeState::Released)
    }

    pub fn get_tasks(&self) -> impl Iterator<Item = &TaskRef> {
        self.tasks.values()
    }

    #[inline]
    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    #[inline]
    pub fn get_task_by_id(&self, id: TaskId) -> Option<&TaskRef> {
        self.tasks.get(&id)
    }

    pub fn get_worker_addresses(&self) -> Map<WorkerId, String> {
        self.workers
            .values()
            .map(|w_ref| {
                let w = w_ref.get();
                (w.id, w.listen_address.clone())
            })
            .collect()
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &WorkerRef {
        self.workers.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_workers(&self) -> impl Iterator<Item = &WorkerRef> {
        self.workers.values()
    }

    #[inline]
    pub fn has_workers(&self) -> bool {
        !self.workers.is_empty()
    }

    fn compute_task(
        &self,
        worker_ref: WorkerRef,
        task_ref: TaskRef,
        notifications: &mut Notifications,
    ) {
        trace_task_assign(task_ref.get().id, worker_ref.get().id);
        notifications.compute_task_on_worker(worker_ref, task_ref);
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
                .get(&assignment.worker)
                .expect("Worker from assignment not found")
                .clone();
            let task_ref = self.get_task_by_id_or_panic(assignment.task);
            task_ref.get_mut().scheduler_priority = assignment.priority;
            let state = {
                let task = task_ref.get();
                match &task.state {
                    TaskRuntimeState::Waiting | TaskRuntimeState::Scheduled(_) => {
                        if task.is_ready() {
                            log::debug!(
                                "Task task={} scheduled & assigned to worker={}",
                                assignment.task,
                                assignment.worker
                            );
                            self.compute_task(worker_ref.clone(), task_ref.clone(), notifications);
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
                        let previous_worker_id = wref.get().id;
                        log::debug!(
                            "Task task={} scheduled to worker={}; stealing (1) from worker={}",
                            assignment.task,
                            assignment.worker,
                            previous_worker_id
                        );
                        if previous_worker_id != assignment.worker {
                            trace_worker_steal(task.id, previous_worker_id, assignment.worker);
                            notifications.steal_task_from_worker(wref.clone(), task_ref.clone());
                            TaskRuntimeState::Stealing(wref.clone(), worker_ref)
                        } else {
                            TaskRuntimeState::Assigned(wref.clone())
                        }
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
                    TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Released => {
                        log::debug!("Rescheduling non-active task={}", assignment.task);
                        continue;
                    }
                }
            };
            task_ref.get_mut().state = state;
        }
    }

    pub fn on_steal_response(
        &mut self,
        worker_ref: &WorkerRef,
        msg: StealResponseMsg,
        notifications: &mut Notifications,
    ) {
        for (task_id, response) in msg.responses {
            log::debug!(
                "Steal response from {}, task={} response={:?}",
                worker_ref.get().id,
                task_id,
                response
            );
            let task_ref = self.get_task_by_id(task_id);
            match task_ref {
                Some(task_ref) => {
                    let new_state = {
                        let task = task_ref.get();
                        if task.is_done() {
                            log::debug!("Received trace response for finished task={}", task_id);
                            trace_worker_steal_response(task.id, worker_ref.get().id, 0, "done");
                            continue;
                        }
                        let to_w = if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
                            assert!(from_w == worker_ref);
                            to_w.clone()
                        } else {
                            panic!(
                                "Invalid state of task={} when steal response occured",
                                task_id
                            );
                        };

                        let success = match response {
                            StealResponse::Ok => true,
                            StealResponse::NotHere | StealResponse::Running => false,
                        };

                        {
                            let from_worker = worker_ref.get();
                            let to_worker = to_w.get();
                            trace_worker_steal_response(
                                task.id,
                                from_worker.id,
                                to_worker.id,
                                if success { "success" } else { "fail" },
                            );
                            notifications.task_steal_response(
                                &from_worker,
                                &to_worker,
                                &task,
                                success,
                            );
                        }

                        if success {
                            log::debug!("Task stealing was successful task={}", task_id);
                            self.compute_task(to_w.clone(), task_ref.clone(), notifications);
                            TaskRuntimeState::Assigned(to_w)
                        } else {
                            log::debug!("Task stealing was not successful task={}", task_id);
                            TaskRuntimeState::Assigned(worker_ref.clone())
                        }
                    };

                    task_ref.get_mut().state = new_state;
                }
                None => {
                    log::debug!("Received trace resposne for invalid task {}", task_id);
                    trace_worker_steal_response_missing(task_id, worker_ref.get().id)
                }
            }
        }
    }

    pub fn on_task_error(
        &mut self,
        worker: &WorkerRef,
        task_id: TaskId,
        error_info: ErrorInfo,
        mut notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_id_or_panic(task_id).clone();
        log::debug!("Task task {} failed", task_id);

        let task_refs: Vec<TaskRef> = {
            assert!(task_ref.get().is_assigned_or_stealed_from(worker));
            task_ref.get().collect_consumers().into_iter().collect()
        };

        self.unregister_as_consumer(&task_ref, &mut notifications);
        for task_ref in &task_refs {
            {
                let task = task_ref.get();
                log::debug!("Task={} canceled because of failed dependency", task.id);
                assert!(task.is_waiting() || task.is_scheduled());
            }
            self.unregister_as_consumer(&task_ref, &mut notifications);
        }

        assert!(matches!(
            self.remove_task(&mut task_ref.get_mut()),
            TaskRuntimeState::Assigned(_)
        ));

        for task_ref in &task_refs {
            // We can drop the resulting state as checks was done earlier
            let _ = self.remove_task(&mut task_ref.get_mut());
        }

        notifications.notify_client_about_task_error(ErrorNotification {
            failed_task: task_ref,
            consumers: task_refs,
            error_info,
        });
    }

    pub fn on_tasks_transferred(
        &mut self,
        worker_ref: &WorkerRef,
        id: TaskId,
        notifications: &mut Notifications,
    ) {
        let worker = worker_ref.get();
        log::debug!("Task id={} transfered on worker={}", id, worker.id);
        // TODO handle the race when task is removed from server before this message arrives
        if let Some(task_ref) = self.get_task_by_id(id) {
            let mut task = task_ref.get_mut();
            match &mut task.state {
                TaskRuntimeState::Finished(_, ws) => {
                    ws.insert(worker_ref.clone());
                }
                TaskRuntimeState::Released
                | TaskRuntimeState::Waiting
                | TaskRuntimeState::Scheduled(_)
                | TaskRuntimeState::Assigned(_)
                | TaskRuntimeState::Stealing(_, _) => {
                    panic!("Invalid task state");
                }
            };
            trace_task_place(task.id, worker.id);
            notifications.task_placed(&worker, &task);
        } else {
            log::debug!("Task id={} is not known to server; replaying with delete", id);
            worker.send_remove_data(id);
        }
    }

    pub fn on_task_finished(
        &mut self,
        worker: &WorkerRef,
        msg: TaskFinishedMsg,
        notifications: &mut Notifications,
    ) {
        let task_ref = self.get_task_by_id_or_panic(msg.id).clone();
        {
            {
                let mut task = task_ref.get_mut();
                let worker_ref = worker.get();
                trace_task_finish(
                    task.id,
                    worker_ref.id,
                    msg.size,
                    (0, 0), /* TODO: gather real computation */
                );
                log::debug!("Task id={} finished on worker={}", task.id, worker_ref.id);
                assert!(task.is_assigned_or_stealed_from(worker));
                let mut set = Set::new();
                set.insert(worker.clone());
                task.state = TaskRuntimeState::Finished(
                    DataInfo {
                        size: msg.size,
                        //r#type: msg.r#type,
                    },
                    set,
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
                            drop(t); // avoid refcell problem
                            self.compute_task(w, consumer.clone(), notifications);
                        }
                    }
                }
                notifications.task_finished(&worker.get(), &task);
                self.unregister_as_consumer(&task_ref, notifications);
                //self.notify_key_in_memory(&task_ref, notifications);
            }

            task_ref
                .get_mut()
                .remove_data_if_possible(self, notifications);
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

    pub fn new_tasks(
        &mut self,
        new_tasks: Vec<TaskRef>,
        notifications: &mut Notifications,
        lowest_id: TaskId,
    ) {
        for task_ref in &new_tasks {
            let task_id = task_ref.get().id;
            log::debug!("New task id={}", task_id);
            trace_task_new(task_id, &task_ref.get().dependencies);
            self.tasks.insert(task_id, task_ref.clone());
        }

        for task_ref in &new_tasks {
            let task = task_ref.get();
            for task_id in &task.dependencies {
                let tr = self.get_task_by_id_or_panic(*task_id);
                tr.get_mut().add_consumer(task_ref.clone());
            }
        }

        let mut count = new_tasks.len();
        let mut processed = Set::with_capacity(count);
        let mut stack: Vec<(TaskRef, usize)> = Default::default();

        for task_ref in new_tasks {
            if !task_ref.get().has_consumers() {
                stack.push((task_ref, 0));
                while let Some((tr, c)) = stack.pop() {
                    let ii = {
                        let task = tr.get();
                        task.dependencies.get(c).copied()
                    };
                    if let Some(inp) = ii {
                        stack.push((tr, c + 1));
                        if inp > lowest_id && processed.insert(inp) {
                            stack.push((self.get_task_by_id_or_panic(inp).clone(), 0));
                        }
                    } else {
                        count -= 1;
                        notifications.new_task(&tr.get());
                    }
                }
            }
        }
        assert_eq!(count, 0);
    }
}

/*
/// Returns task duration as specified by Dask.
/// Converts from UNIX in seconds to a microseconds.
fn get_task_duration(msg: &TaskFinishedMsg) -> (u64, u64) {
    msg.startstops
        .iter()
        .find(|map| map[b"action" as &[u8]].as_str().unwrap() == "compute")
        .map(|map| {
            (
                (map[b"start" as &[u8]].as_f64().unwrap() * 1_000_000f64) as u64,
                (map[b"stop" as &[u8]].as_f64().unwrap() * 1_000_000f64) as u64,
            )
        })
        .unwrap_or((0, 0))
}*/

#[cfg(test)]
mod tests {
    use crate::common::Set;
    use crate::scheduler::protocol::{TaskUpdate, TaskUpdateType};
    use crate::scheduler::ToSchedulerMessage;
    use crate::server::notifications::Notifications;
    use crate::server::protocol::messages::worker::TaskFinishedMsg;
    use crate::server::task::{ErrorInfo, TaskRuntimeState};
    use crate::test_util::{task_add, task_add_deps, task_assign, worker, TestClientStateRef};

    #[test]
    fn add_remove() {
        let core_ref = TestClientStateRef::new().get_core();
        let mut core = core_ref.get_mut();
        let t = task_add(&mut core, 101);
        assert_eq!(core.get_task_by_id(101).unwrap(), &t);
        assert!(match core.remove_task(&mut t.get_mut()) {
            TaskRuntimeState::Waiting => true,
            _ => false,
        });
        assert_eq!(core.get_task_by_id(101), None);
    }

    #[test]
    fn assign_task() {
        let core_ref = TestClientStateRef::new().get_core();
        let mut core = core_ref.get_mut();
        let t = task_add(&mut core, 101);
        let (w, _w_rx) = worker(&mut core, "w0");
        let notifications = task_assign(&mut core, &t, &w);
        assert!(t.get().is_assigned_or_stealed_from(&w));
        assert_eq!(notifications.workers[&w].compute_tasks, vec!(t));
    }

    #[test]
    fn finish_task_scheduler_notification() {
        let core_ref = TestClientStateRef::new().get_core();
        let mut core = core_ref.get_mut();

        let t = task_add(&mut core, 101);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let mut notifications = Notifications::default();
        let nbytes = 16;
        core.on_task_finished(&w, TaskFinishedMsg { id: 101, nbytes }, &mut notifications);
        assert_eq!(
            notifications.scheduler_messages[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: t.get().id,
                state: TaskUpdateType::Finished,
                worker: w.get().id,
                size: Some(nbytes),
            })
        );
    }

    #[test]
    fn release_task_without_consumers() {
        let core_ref = TestClientStateRef::new().get_core();
        let mut core = core_ref.get_mut();
        let t = task_add(&mut core, 101);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let _type = vec![1, 2, 3];
        let nbytes = 16;

        let mut notifications = Notifications::default();
        core.on_task_finished(&w, TaskFinishedMsg { id: 101, nbytes }, &mut notifications);
        match &t.get().state {
            TaskRuntimeState::Released => {}
            _ => panic!("Wrong task state"),
        };
    }

    #[test]
    fn finish_task_with_consumers() {
        let core_ref = TestClientStateRef::new().get_core();
        let mut core = core_ref.get_mut();
        let t = task_add(&mut core, 101);
        let _ = task_add_deps(&mut core, 1, &[&t]);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let _type = vec![1, 2, 3];
        let nbytes = 16;

        let mut notifications = Notifications::default();
        core.on_task_finished(&w, TaskFinishedMsg { id: 101, nbytes }, &mut notifications);
        match &t.get().state {
            TaskRuntimeState::Finished(data, workers) => {
                assert_eq!(data.size, nbytes);
                let mut s = Set::new();
                s.insert(w);
                assert_eq!(workers, &s);
            }
            _ => panic!("Wrong task state"),
        };
    }

    #[test]
    fn finish_task_inmemory_notification() {
        let cs = TestClientStateRef::new();
        let core_ref = cs.get_core();
        let mut core = core_ref.get_mut();

        let t = task_add(&mut core, 101);
        let (w, _w_rx) = worker(&mut core, "w0");
        task_assign(&mut core, &t, &w);

        let mut notifications = Notifications::default();
        core.on_task_finished(
            &w,
            TaskFinishedMsg { id: 101, nbytes: 0 },
            &mut notifications,
        );
        assert_eq!(notifications.client_notifications.finished_tasks, vec![101]);
    }

    #[test]
    #[should_panic]
    fn finish_unassigned_task() {
        let cs = TestClientStateRef::new();
        let core_ref = cs.get_core();
        let mut core = core_ref.get_mut();

        task_add(&mut core, 101);

        let (w, _w_rx) = worker(&mut core, "w0");

        core.on_task_finished(
            &w,
            TaskFinishedMsg {
                id: 101,
                nbytes: 16,
            },
            &mut &mut Default::default(),
        );
    }

    #[test]
    #[should_panic]
    fn error_unassigned_task() {
        let cs = TestClientStateRef::new();
        let core_ref = cs.get_core();
        let mut core = core_ref.get_mut();

        task_add(&mut core, 101);

        let (w, _w_rx) = worker(&mut core, "w0");

        core.on_task_error(
            &w,
            101,
            ErrorInfo {
                exception: Default::default(),
                traceback: Default::default(),
            },
            &mut Default::default(),
        );
    }

    #[test]
    fn task_duration() {
        /*assert_eq!(
            get_task_duration(&TaskFinishedMsg {
                key: "null".into(),
                nbytes: 16,
                r#type: vec![1, 2, 3],
                startstops: vec!(
                    startstop_item("send", 100.0, 200.0),
                    startstop_item("compute", 200.134, 300.456)
                ),
            }),
            (200134000, 300456000)
        );*/
    }

    #[test]
    fn task_duration_missing() {
        /*assert_eq!(
            get_task_duration(&TaskFinishedMsg {
                key: "null".into(),
                nbytes: 16,
                r#type: vec![1, 2, 3],
                startstops: vec!(),
            }),
            (0, 0)
        );*/
    }
}
