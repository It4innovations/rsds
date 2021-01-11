use crate::scheduler::TaskId;
use crate::server::dask::client::ClientId;
use crate::server::dask::key::DaskKey;
use crate::server::dask::state::DaskStateRef;
use crate::server::gateway::Gateway;
use crate::server::task::ErrorInfo;

pub struct DaskGateway {
    pub dask_state_ref: DaskStateRef,
}

impl Gateway for DaskGateway {
    fn is_kept(&self, task_id: u64) -> bool {
        let dask_state = self.dask_state_ref.get();
        dask_state.subscriptions().contains_key(&task_id)
    }

    fn send_client_task_finished(&mut self, task_id: TaskId) {
        let dask_state = self.dask_state_ref.get();
        if let Some(clients) = dask_state.subscriptions().get(&task_id) {
            for client_id in clients {
                let key = dask_state.get_task_key(task_id).unwrap();
                dask_state
                    .get_client_by_id_or_panic(*client_id)
                    .send_finished_keys(vec![key.clone()])
                    .unwrap();
            }
        }
    }

    fn send_client_task_removed(&mut self, task_id: TaskId) {
        let mut dask_state = self.dask_state_ref.get_mut();
        dask_state.remove_task_id(task_id);
    }

    fn send_client_task_error(
        &mut self,
        failed_task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: ErrorInfo,
    ) {
        let mut dask_state = self.dask_state_ref.get_mut();
        log::debug!(
            "Dask task key={} failed",
            dask_state.get_task_key(failed_task_id).unwrap()
        );

        let mut clients = Vec::<(ClientId, DaskKey)>::new();

        let mut process = |task_id: TaskId| {
            if let Some(ids) = dask_state.remove_all_subscriptions(task_id) {
                for client_id in ids {
                    let key = dask_state.get_task_key(task_id).unwrap().clone();
                    clients.push((client_id, key));
                }
            }
            dask_state.remove_task_id(task_id);
        };

        for task_id in consumers_id {
            process(task_id);
        }
        process(failed_task_id);

        if clients.is_empty() {
            log::warn!("Task failed, but no client is listening");
        }

        for (client_id, key) in clients {
            log::debug!(
                "Dask task={} key={} failed; announcing to client={}",
                failed_task_id,
                &key,
                client_id
            );
            let client = dask_state.get_client_by_id_or_panic(client_id);
            client.send_error(key, &error_info).unwrap();
        }
    }
}
