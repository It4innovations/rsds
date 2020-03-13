use crate::scheduler::graph::{assign_task_to_worker, create_task_assignment, SchedulerGraph};

use crate::scheduler::metrics::NodeMetrics;
use crate::scheduler::protocol::SchedulerRegistration;
use crate::scheduler::task::{Task, TaskRef};
use crate::scheduler::utils::task_transfer_cost;
use crate::scheduler::worker::{Worker, WorkerRef};
use crate::scheduler::{Scheduler, TaskAssignment, ToSchedulerMessage};

#[derive(Default, Debug)]
pub struct LevelScheduler<Metric> {
    graph: SchedulerGraph,
    _phantom: std::marker::PhantomData<Metric>,
}

impl<Metric: NodeMetrics> Scheduler for LevelScheduler<Metric> {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "blevel".into(),
            scheduler_version: "0.0".into(),
        }
    }

    fn handle_messages(&mut self, messages: Vec<ToSchedulerMessage>) -> bool {
        for message in messages {
            self.graph.handle_message(message);
        }
        !self.graph.ready_to_assign.is_empty()
    }

    fn schedule(&mut self) -> Vec<TaskAssignment> {
        let mut assignments = vec![];

        if !self.graph.new_tasks.is_empty() {
            Metric::assign_metric(&self.graph.tasks);
            self.graph.new_tasks.clear();
        }

        if !self.graph.ready_to_assign.is_empty() {
            let mut underloaded_workers: Vec<_> = self
                .graph
                .workers
                .values()
                .filter(|w| w.get().is_underloaded())
                .collect();
            if !underloaded_workers.is_empty() {
                self.graph
                    .ready_to_assign
                    .sort_unstable_by_key(|t| Metric::SORT_MULTIPLIER * t.get().computed_metric);

                // TODO: handle multi-CPU workers
                let end =
                    std::cmp::min(self.graph.ready_to_assign.len(), underloaded_workers.len());
                for tref in self.graph.ready_to_assign.drain(..end) {
                    let mut task = tref.get_mut();
                    let (windex, worker) = underloaded_workers
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, w)| task_transfer_cost(&task, &w))
                        .unwrap();
                    assign(
                        &mut task,
                        tref.clone(),
                        &mut worker.get_mut(),
                        (*worker).clone(),
                        &mut assignments,
                    );
                    underloaded_workers.swap_remove(windex);
                }
            }
        }
        assignments
    }
}

fn assign(
    task: &mut Task,
    task_ref: TaskRef,
    worker: &mut Worker,
    worker_ref: WorkerRef,
    assignments: &mut Vec<TaskAssignment>,
) {
    assign_task_to_worker(task, task_ref.clone(), worker, worker_ref);
    assignments.push(create_task_assignment(&task_ref));
}
