use crate::worker::state::WorkerState;
use crate::worker::subworker::SubworkerRef;

pub fn choose_subworker(state: &mut WorkerState) -> SubworkerRef {
    // TODO: Real implementation
    state.free_subworkers.pop().unwrap()
}

pub fn try_start_tasks(state: &mut WorkerState) {
    //log::debug!("XX {} {}", state.free_subworkers.len(), state.subworkers.len());
    if state.free_subworkers.is_empty() {
        return;
    }
    while let Some((task_ref, _)) = state.ready_task_queue.pop() {
        {
            let subworker_ref = choose_subworker(state);
            let mut task = task_ref.get_mut();
            task.set_running(subworker_ref.clone());
            let mut sw = subworker_ref.get_mut();
            assert!(sw.running_task.is_none());
            sw.running_task = Some(task_ref.clone());
            sw.start_task(&task);
        }
        if state.free_subworkers.is_empty() {
            return;
        }
    }
}

/*pub fn try_download_tasks(state: &mut WorkerState, stream: ()) {
    while state.
}*/
