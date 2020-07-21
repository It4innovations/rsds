use crate::common::WrappedRcRefCell;
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::messages::worker::ComputeTaskMsg;
use crate::server::protocol::Priority;
use crate::worker::subworker::SubworkerRef;

pub enum TaskState {
    Waiting,
    Running(SubworkerRef),
}

pub struct Task {
    pub key: DaskKey,
    pub state: TaskState,
    pub priority: (Priority, Priority),
    pub function: rmpv::Value,
    pub args: rmpv::Value,
    pub kwargs: Option<rmpv::Value>,
}

impl Task {
    #[inline]
    pub fn is_waiting(&self) -> bool {
        match self.state {
            TaskState::Waiting => true,
            _ => false,
        }
    }

    pub fn set_running(&mut self, subworker_ref: SubworkerRef) {
        assert!(self.is_waiting());
        self.state = TaskState::Running(subworker_ref);
    }
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(message: ComputeTaskMsg) -> Self {
        TaskRef::wrap(Task {
            key: message.key,
            function: message.function,
            args: message.args,
            kwargs: message.kwargs,
            priority: (message.user_priority, message.scheduler_priority),
            state: TaskState::Waiting,
        })
    }
}
