use crate::scheduler::TaskId;
use crate::server::notifications::ClientNotifications;
use crate::server::task::Task;
use std::rc::Rc;

pub trait Gateway {
    fn is_kept(&self, task_id: TaskId) -> bool;
    fn send_notifications(&self, notifications: ClientNotifications);
}
