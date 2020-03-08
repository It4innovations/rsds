use crate::scheduler::graph::{assign_task_to_worker, Notifications, SchedulerGraph};

use crate::scheduler::protocol::SchedulerRegistration;
use crate::scheduler::utils::{compute_b_level, task_transfer_cost};
use crate::scheduler::{Scheduler, SchedulerSender, ToSchedulerMessage};

#[derive(Default, Debug)]
pub struct BlevelScheduler {
    graph: SchedulerGraph,
    notifications: Notifications,
}

impl Scheduler for BlevelScheduler {
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

    fn schedule(&mut self, sender: &mut SchedulerSender) {
        self.notifications.clear();

        if !self.graph.new_tasks.is_empty() {
            compute_b_level(&self.graph.tasks);
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
                // Larger B-level goes first
                self.graph
                    .ready_to_assign
                    .sort_unstable_by_key(|t| -t.get().b_level);

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
                    assign_task_to_worker(
                        &mut task,
                        tref.clone(),
                        &mut worker.get_mut(),
                        (*worker).clone(),
                        &mut self.notifications,
                    );
                    underloaded_workers.swap_remove(windex);
                }
            }
        }

        self.graph.send_notifications(&self.notifications, sender);
    }
}
