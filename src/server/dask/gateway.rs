use crate::common::Map;
use crate::server::dask::client::ClientId;
use crate::server::dask::key::DaskKey;
use crate::server::dask::state::DaskStateRef;
use crate::server::gateway::Gateway;
use crate::server::notifications::ClientNotifications;
use crate::server::task::TaskRef;

pub struct DaskGateway {
    pub dask_state_ref: DaskStateRef,
}

impl Gateway for DaskGateway {
    fn is_kept(&self, task_id: u64) -> bool {
        let dask_state = self.dask_state_ref.get();
        dask_state.subscriptions().contains_key(&task_id)
    }

    fn send_notifications(&self, notifications: ClientNotifications) {
        let mut dask_state = self.dask_state_ref.get_mut();

        for error_notification in notifications.error_notifications {
            let failed_task_id = error_notification.failed_task.get().id;
            log::debug!(
                "Dask task key={} failed",
                dask_state.get_task_key(failed_task_id).unwrap()
            );

            let mut clients = Vec::<(ClientId, DaskKey)>::new();

            let mut process = |task_ref: &TaskRef| {
                let task_id = task_ref.get().id;
                if let Some(ids) = dask_state.remove_all_subscriptions(task_id) {
                    for client_id in ids {
                        let key = dask_state.get_task_key(task_id).unwrap().clone();
                        clients.push((client_id, key));
                    }
                }
                dask_state.remove_task_id(task_id);
            };

            for task_ref in &error_notification.consumers {
                process(task_ref);
            }
            process(&error_notification.failed_task);

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
                client
                    .send_error(key, &error_notification.error_info)
                    .unwrap();
            }
        }

        let mut subscribed_finished_keys: Map<ClientId, Vec<DaskKey>> = Map::new();
        for task_id in notifications.finished_tasks {
            if let Some(clients) = dask_state.subscriptions().get(&task_id) {
                let key = dask_state.get_task_key(task_id).unwrap();
                for client_id in clients {
                    subscribed_finished_keys
                        .entry(*client_id)
                        .or_default()
                        .push(key.clone());
                }
            }
        }

        for (client_id, keys) in subscribed_finished_keys {
            let client = dask_state.get_client_by_id_or_panic(client_id);
            client.send_finished_keys(keys).unwrap();
        }

        for task_id in notifications.removed_tasks {
            dask_state.remove_task_id(task_id);
        }
    }
}
