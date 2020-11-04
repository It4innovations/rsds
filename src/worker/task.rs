use crate::common::WrappedRcRefCell;
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::messages::worker::ComputeTaskMsg;
use crate::server::protocol::PriorityValue;
use crate::worker::subworker::SubworkerRef;
use crate::worker::data::DataObjectRef;

pub enum TaskState {
    Waiting(u32),
    Running(SubworkerRef),
    Removed,
}

pub struct Task {
    pub key: DaskKey,
    pub state: TaskState,
    pub priority: (PriorityValue, PriorityValue),
    pub deps: Vec<DataObjectRef>,
    pub function: rmpv::Value,
    pub args: rmpv::Value,
    pub kwargs: Option<rmpv::Value>,
}

impl Task {
    #[inline]
    pub fn is_waiting(&self) -> bool {
        match self.state {
            TaskState::Waiting(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        match self.state {
            TaskState::Waiting(0) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        match self.state {
            TaskState::Running(_) => true,
            _ => false,
        }
    }

    pub fn get_waiting(&self) -> u32 {
        match self.state {
            TaskState::Waiting(x) => x,
            _ => 0,
        }
    }

    pub fn decrease_waiting_count(&mut self) -> bool {
        match &mut self.state {
            TaskState::Removed | TaskState::Waiting(0) | TaskState::Running(_) => {
                unreachable!()
            },
            TaskState::Waiting(ref mut x) => {
                *x -= 1;
                *x == 0
            }
        }
    }

    pub fn increase_waiting_count(&mut self) {
        match &mut self.state {
            TaskState::Waiting(ref mut x) => {
                *x += 1;
            }
            TaskState::Running(_) | TaskState::Removed => {
                unreachable!()
            }
        }
    }

    pub fn set_running(&mut self, subworker_ref: SubworkerRef) {
        assert!(self.is_ready());
        self.state = TaskState::Running(subworker_ref);
    }
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(message: ComputeTaskMsg) -> Self {
        let task_ref = TaskRef::wrap(Task {
            key: message.key,
            function: message.function,
            args: message.args,
            kwargs: message.kwargs,
            priority: (message.user_priority, message.scheduler_priority),
            state: TaskState::Waiting(0),
            deps: Default::default()
        });
        task_ref
    }
}
