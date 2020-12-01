use crate::worker::state::WorkerState;
use crate::worker::subworker::SubworkerRef;
use crate::server::worker::WorkerRef;
use crate::scheduler::TaskId;
use crate::common::Map;
use crate::worker::task::Task;
use crate::server::core::CoreRef;
use bytes::BytesMut;

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

fn scatter_tasks<T>(
    data: Vec<(TaskId, T)>,
    workers: &[WorkerRef],
    counter: usize,
) -> Vec<(WorkerRef, Vec<(TaskId, T)>)> {
    let total_cpus: usize = workers.iter().map(|wr| wr.get().ncpus as usize).sum();
    let mut counter = counter % total_cpus;

    let mut cpu = 0;
    let mut index = 0;
    for (i, wr) in workers.iter().enumerate() {
        let ncpus = wr.get().ncpus as usize;
        if counter >= ncpus {
            counter -= ncpus;
        } else {
            cpu = counter;
            index = i;
            break;
        }
    }

    let mut worker_ref = &workers[index];
    let mut ncpus = worker_ref.get().ncpus as usize;

    let mut result: Map<WorkerRef, Vec<(TaskId, T)>> = Map::new();

    for (key, keydata) in data {
        result
            .entry(worker_ref.clone())
            .or_default()
            .push((key, keydata));
        cpu += 1;
        if cpu >= ncpus {
            cpu = 0;
            index += 1;
            index %= workers.len();
            worker_ref = &workers[index];
            ncpus = worker_ref.get().ncpus as usize;
        }
    }
    result.into_iter().collect()
}

pub async fn scatter(core_ref: &CoreRef, workers: &[WorkerRef], data: Vec<(TaskId, BytesMut)>) {
    let counter = core_ref.get_mut().get_and_move_scatter_counter(data.len());
    todo!();
    /*
    let worker_futures = join_all(
        who_what
            .into_iter()
            .map(|(worker, data)| update_data_on_worker(worker.get().address().into(), data)),
    );*/
}