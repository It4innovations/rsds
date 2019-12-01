use std::collections::{HashSet, HashMap};

use crate::scheduler::schedproto::{TaskId, TaskInfo};
use crate::scheduler::implementation::worker::WorkerRef;

pub enum SchedulerTaskState {
    Waiting,
    Finished,
}

pub struct Task {
    pub id: TaskId,
    pub state: SchedulerTaskState,
    pub inputs: Vec<TaskRef>,
    pub consumers: HashSet<TaskRef>,
    pub b_level: f32,
    pub unfinished_deps: u32,
    pub assigned_worker: Option<WorkerRef>,
    pub placement: HashSet<WorkerRef>,
    pub size: u64,
    pub take_flag: bool  // Used in algorithms, no meaning between calls
}

pub type TaskRef = crate::common::WrappedRcRefCell<Task>;

impl Task {

    #[inline]
    pub fn is_waiting(&self) -> bool {
        match self.state {
            SchedulerTaskState::Waiting => true,
            _ => false
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.state {
            SchedulerTaskState::Finished => true,
            _ => false
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_deps == 0
    }

    pub fn sanity_check(&self, task_ref: &TaskRef) {
        let mut unfinished = 0;
        for inp in &self.inputs {
            let ti = inp.get();
            if let SchedulerTaskState::Waiting = ti.state {
                unfinished += 1;
            }
            assert!(ti.consumers.contains(task_ref));

        }
        assert_eq!(unfinished, self.unfinished_deps);

        match self.state {
            SchedulerTaskState::Waiting => {
                for c in &self.consumers {
                    assert!(c.get().is_waiting());
                }
            },
            SchedulerTaskState::Finished => {
                for inp in &self.inputs {
                    assert!(inp.get().is_finished());
                }
            },
        };
    }
}

impl TaskRef {
    pub fn new(ti: TaskInfo, inputs: Vec<TaskRef>) -> Self {
        let mut unfinished_deps = 0;
        for inp in &inputs {
            let t = inp.get();
            if t.is_waiting() {
                unfinished_deps += 1;
            } else {
                assert!(t.is_finished());
            }
        }
        let task_ref = Self::wrap(Task {
            id: ti.id,
            inputs,
            state: SchedulerTaskState::Waiting,
            b_level: 0.0,
            unfinished_deps,
            size: 0u64,
            consumers: Default::default(),
            assigned_worker: None,
            placement: Default::default(),
            take_flag: false,
        });
        {
            let task = task_ref.get();
            for inp in &task.inputs {
                let mut t = inp.get_mut();
                t.consumers.insert(task_ref.clone());
            }
        }
        task_ref
    }
}
