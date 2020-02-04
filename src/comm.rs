use futures::{Stream, Sink, StreamExt, SinkExt, FutureExt};
use crate::core::{CoreRef, Core};
use crate::reactor::{release_keys, update_graph};
use crate::protocol::generic::RegisterWorkerMsg;
use crate::worker::{create_worker, WorkerRef};
use crate::notifications::{Notifications, ClientNotification, WorkerNotification};
use crate::task::ErrorInfo;
use crate::client::{Client, ClientId};
use crate::protocol::workermsg::{FromWorkerMessage, Status, ToWorkerMessage, DeleteDataMsg, StealRequestMsg};
use crate::protocol::clientmsg::{FromClientMessage, ToClientMessage, KeyInMemoryMsg, TaskErredMsg};
use crate::protocol::protocol::{Batch, DaskPacket};
use crate::common::{WrappedRcRefCell, Map};
use crate::scheduler::ToSchedulerMessage;
use tokio::sync::mpsc::UnboundedSender;
use crate::protocol::protocol::MessageBuilder;
use crate::task::TaskRuntimeState;

pub type CommRef = WrappedRcRefCell<Comm>;

pub struct Comm {
    sender: UnboundedSender<Vec<ToSchedulerMessage>>
}

impl Comm {
    pub fn send(&mut self, core: &mut Core, notifications: Notifications) -> crate::Result<()> {
        if !notifications.scheduler_messages.is_empty() {
            self.sender.send(notifications.scheduler_messages).unwrap();
        }

        self.send_to_workers(&core, notifications.workers)?;
        self.send_to_clients(core, notifications.clients)?;
        Ok(())
    }

    fn send_to_clients(&mut self, core: &mut Core, notifications: Map<ClientId, ClientNotification>) -> crate::Result<()> {
        for (client_id, c_update) in notifications {
            let mut mbuilder =
                MessageBuilder::<ToClientMessage>::with_capacity(c_update.error_tasks.len() + c_update.in_memory_tasks.len());
            for task_ref in c_update.error_tasks {
                let task = task_ref.get();
                if let TaskRuntimeState::Error(error_info) = &task.state {
                    let exception = mbuilder.copy_serialized(&error_info.exception);
                    let traceback = mbuilder.copy_serialized(&error_info.traceback);
                    mbuilder.add_message(ToClientMessage::TaskErred(TaskErredMsg {
                        key: task.key.clone(),
                        exception,
                        traceback,
                    }));
                } else {
                    panic!("Task is not in error state");
                };
            }

            for task_ref in c_update.in_memory_tasks {
                let task = task_ref.get();
                mbuilder.add_message(ToClientMessage::KeyInMemory(KeyInMemoryMsg {
                    key: task.key.clone(),
                    r#type: task.data_info().unwrap().r#type.clone(),
                }));
            }

            if !mbuilder.is_empty() {
                let client = core.get_client_by_id_or_panic(client_id);
                client.send_dask_packet(mbuilder.build_batch()?)?;
            }
        }
        Ok(())
    }
    fn send_to_workers(&mut self, core: &Core, notifications: Map<WorkerRef, WorkerNotification>) -> crate::Result<()> {
        for (worker_ref, w_update) in notifications {
            let mut mbuilder = MessageBuilder::new();

            for task in w_update.compute_tasks {
                task.get().make_compute_task_msg(core, &mut mbuilder);
            }

            if !w_update.delete_keys.is_empty() {
                mbuilder.add_message(ToWorkerMessage::DeleteData(DeleteDataMsg {
                    keys: w_update.delete_keys,
                    report: false,
                }));
            }

            for tref in w_update.steal_tasks {
                let task = tref.get();
                mbuilder.add_message(ToWorkerMessage::StealRequest(StealRequestMsg {
                    key: task.key.clone(),
                }));
            }

            if !mbuilder.is_empty() {
                let mut worker = worker_ref.get_mut();
                worker.send_dask_packet(mbuilder.build_batch()?)
                    .unwrap_or_else(|_| {
                        // !!! Do not propagate error right now, we need to finish sending protocol to others
                        // Worker cleanup is done elsewhere (when worker future terminates),
                        // so we can safely ignore this. Since we are nice guys we log (debug) message.
                        log::debug!("Sending tasks to worker {} failed", worker.id);
                    });
            }
        }

        Ok(())
    }
}

impl CommRef {
    pub fn new(sender: UnboundedSender<Vec<ToSchedulerMessage>>) -> Self {
        WrappedRcRefCell::wrap(Comm {
            sender
        })
    }
}

pub async fn worker_rpc_loop<
    Reader: Stream<Item = crate::Result<Batch<FromWorkerMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::DsError> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    mut sender: Writer,
    msg: RegisterWorkerMsg,
) -> crate::Result<()> {
    let (queue_sender, mut queue_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let (worker_id, worker_ref) = create_worker(core_ref, queue_sender, msg.address);
    let mut notifications = Notifications::default();
    notifications.new_worker(&worker_ref.get());
    comm_ref.get_mut().send(&mut core_ref.get_mut(), notifications).unwrap();

    log::info!("Worker {} registered from {}", worker_id, address);

    let snd_loop = async move {
        while let Some(data) = queue_receiver.next().await {
            if let Err(e) = sender.send(data).await {
                log::error!("Send to worker failed");
                return Err(e);
            }
        }
        Ok(())
    };

    let core_ref2 = core_ref.clone();
    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let mut notifications = Notifications::default();

            let messages = messages?;
            for message in messages {
                log::debug!("Worker recv message {:?}", message);
                match message {
                    FromWorkerMessage::TaskFinished(msg) => {
                        assert_eq!(msg.status, Status::Ok); // TODO: handle other cases ??
                        let mut core = core_ref.get_mut();
                        core.on_task_finished(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::AddKeys(msg) => {
                        let mut core = core_ref.get_mut();
                        core.on_tasks_transferred(&worker_ref, msg.keys, &mut notifications);
                    }
                    FromWorkerMessage::TaskErred(msg) => {
                        assert_eq!(msg.status, Status::Error); // TODO: handle other cases ??
                        let error_info = ErrorInfo {
                            exception: msg.exception,
                            traceback: msg.traceback,
                        };
                        let mut core = core_ref.get_mut();
                        core.on_task_error(&worker_ref, msg.key, error_info, &mut notifications);
                        // TODO: Inform scheduler
                    }
                    FromWorkerMessage::StealResponse(msg) => {
                        let mut core = core_ref.get_mut();
                        core.on_steal_response(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::KeepAlive => { /* Do nothing by design */ }
                    FromWorkerMessage::Release(_) => { /* Do nothing TODO */ }
                    FromWorkerMessage::Unregister => break 'outer,
                }
            }
            let mut core = core_ref.get_mut();
            comm_ref.get_mut().send(&mut core, notifications).unwrap();
        }
        Ok(())
    };

    let result = futures::future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
        log::error!(
            "Error in worker connection (id={}, connection={}): {}",
            worker_id,
            address,
            e
        );
    }
    log::info!(
        "Worker {} connection closed (connection: {})",
        worker_id,
        address
    );
    let mut core = core_ref2.get_mut();
    core.unregister_worker(worker_id);
    Ok(())
}

pub async fn client_rpc_loop<
    Reader: Stream<Item = crate::Result<Batch<FromClientMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::DsError> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    mut sender: Writer,
    client_key: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let client_id = {
        let mut core = core_ref.get_mut();
        let client_id = core.new_client_id();
        let client = Client::new(client_id, client_key, snd_sender);
        core.register_client(client);
        client_id
    };

    log::info!("Client {} registered from {}", client_id, address);

    let snd_loop = async move {
        while let Some(data) = snd_receiver.next().await {
            if let Err(e) = sender.send(data).await {
                return Err(e);
            }
        }
        Ok(())
    };

    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let messages = messages?;
            for message in messages {
                log::debug!("Client recv message {:?}", message);
                match message {
                    FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                    FromClientMessage::ClientReleasesKeys(msg) => {
                        release_keys(&core_ref, &comm_ref, msg.client, msg.keys);
                    }
                    FromClientMessage::UpdateGraph(update) => {
                        update_graph(&core_ref, &comm_ref, client_id, update);
                    }
                    FromClientMessage::CloseClient => {
                        log::debug!("CloseClient message received");
                        break 'outer;
                    }
                    _ => {
                        panic!("Unhandled client message: {:?}", message);
                    }
                }
            }
        }
        Ok(())
    };

    let result = futures::future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
        log::error!(
            "Error in client connection (id={}, connection={}): {}",
            client_id,
            address,
            e
        );
    }
    log::info!(
        "Client {} connection closed (connection: {})",
        client_id,
        address
    );
    let mut core = core_ref2.get_mut();
    core.unregister_client(client_id);
    Ok(())
}
