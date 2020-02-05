use crate::client::{Client, ClientId};
use crate::common::{Map, WrappedRcRefCell};
use crate::core::{Core, CoreRef};
use crate::protocol::clientmsg::{
    FromClientMessage, KeyInMemoryMsg, TaskErredMsg, ToClientMessage,
};
use crate::protocol::generic::{
    GenericMessage, IdentityResponse, RegisterWorkerMsg, SimpleMessage, WorkerInfo,
};
use crate::protocol::protocol::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, serialize_batch_packet,
    serialize_single_packet, MessageBuilder,
};
use crate::protocol::protocol::{Batch, DaskPacket};
use crate::protocol::workermsg::{
    DeleteDataMsg, FromWorkerMessage, RegisterWorkerResponseMsg, Status, StealRequestMsg,
    ToWorkerMessage,
};
use crate::reactor::{gather, get_ncores, release_keys, scatter, update_graph, who_has};
use crate::scheduler::schedproto::{TaskStealResponse, TaskUpdate, TaskUpdateType};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::task::TaskRuntimeState;
use crate::task::{ErrorInfo, Task, TaskRef};
use crate::worker::Worker;
use crate::worker::{create_worker, WorkerRef};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use smallvec::smallvec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use crate::protocol::key::{DaskKey, to_dask_key};

pub type CommRef = WrappedRcRefCell<Comm>;

pub struct Comm {
    sender: UnboundedSender<Vec<ToSchedulerMessage>>,
}

impl Comm {
    pub fn notify(&mut self, core: &mut Core, notifications: Notifications) -> crate::Result<()> {
        if !notifications.scheduler_messages.is_empty() {
            self.sender.send(notifications.scheduler_messages).unwrap();
        }

        self.notify_workers(&core, notifications.workers)?;
        self.notify_clients(core, notifications.clients)?;
        Ok(())
    }

    fn notify_clients(
        &mut self,
        core: &mut Core,
        notifications: Map<ClientId, ClientNotification>,
    ) -> crate::Result<()> {
        for (client_id, c_update) in notifications {
            let mut mbuilder = MessageBuilder::<ToClientMessage>::with_capacity(
                c_update.error_tasks.len() + c_update.in_memory_tasks.len(),
            );
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
                self.send_client_packet(
                    core.get_client_by_id_or_panic(client_id),
                    mbuilder.build_batch()?,
                )?;
            }
        }
        Ok(())
    }
    fn notify_workers(
        &mut self,
        core: &Core,
        notifications: Map<WorkerRef, WorkerNotification>,
    ) -> crate::Result<()> {
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
                self.send_worker_packet(&worker_ref, mbuilder.build_batch()?)
                    .unwrap_or_else(|_| {
                        // !!! Do not propagate error right now, we need to finish sending protocol to others
                        // Worker cleanup is done elsewhere (when worker future terminates),
                        // so we can safely ignore this. Since we are nice guys we log (debug) message.
                        log::debug!("Sending tasks to worker {} failed", worker_ref.get().id);
                    });
            }
        }

        Ok(())
    }

    #[inline]
    fn send_worker_packet(&mut self, worker: &WorkerRef, packet: DaskPacket) -> crate::Result<()> {
        worker.get_mut().send_dask_packet(packet)
    }
    #[inline]
    fn send_client_packet(&mut self, client: &mut Client, packet: DaskPacket) -> crate::Result<()> {
        client.send_dask_packet(packet)
    }
}

impl CommRef {
    pub fn new(sender: UnboundedSender<Vec<ToSchedulerMessage>>) -> Self {
        Self::wrap(Comm { sender })
    }
}

#[derive(Default, Debug)]
pub(crate) struct WorkerNotification {
    pub(crate) compute_tasks: Vec<TaskRef>,
    pub(crate) delete_keys: Vec<DaskKey>,
    pub(crate) steal_tasks: Vec<TaskRef>,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Default, Debug)]
pub(crate) struct ClientNotification {
    pub(crate) in_memory_tasks: Vec<TaskRef>,
    pub(crate) error_tasks: Vec<TaskRef>,
}

#[derive(Default, Debug)]
pub struct Notifications {
    pub(crate) workers: Map<WorkerRef, WorkerNotification>,
    pub(crate) clients: Map<ClientId, ClientNotification>,
    pub(crate) scheduler_messages: Vec<ToSchedulerMessage>,
}

impl Notifications {
    pub fn new_worker(&mut self, worker: &Worker) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewWorker(worker.make_sched_info()));
    }

    #[inline]
    pub fn new_task(&mut self, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::NewTask(task.make_sched_info()));
    }

    pub fn delete_key_from_worker(&mut self, worker_ref: WorkerRef, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Removed,
                id: task.id,
                worker: worker_ref.get().id,
                size: None,
            }));
        self.workers
            .entry(worker_ref)
            .or_default()
            .delete_keys
            .push(task.key.clone());
    }

    pub fn task_placed(&mut self, worker: &Worker, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Placed,
                id: task.id,
                worker: worker.id,
                size: None,
            }));
    }

    pub fn task_finished(&mut self, worker: &Worker, task: &Task) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Finished,
                id: task.id,
                worker: worker.id,
                size: Some(task.data_info().unwrap().size),
            }));
    }

    pub fn task_steal_response(
        &mut self,
        from_worker: &Worker,
        to_worker: &Worker,
        task: &Task,
        success: bool,
    ) {
        self.scheduler_messages
            .push(ToSchedulerMessage::TaskStealResponse(TaskStealResponse {
                id: task.id,
                from_worker: from_worker.id,
                to_worker: to_worker.id,
                success,
            }));
    }

    pub fn compute_task_on_worker(&mut self, worker_ref: WorkerRef, task_ref: TaskRef) {
        self.workers
            .entry(worker_ref)
            .or_default()
            .compute_tasks
            .push(task_ref);
    }

    pub fn steal_task_from_worker(&mut self, worker_ref: WorkerRef, task_ref: TaskRef) {
        self.workers
            .entry(worker_ref)
            .or_default()
            .steal_tasks
            .push(task_ref);
    }

    pub fn notify_client_about_task_error(&mut self, client_id: ClientId, task_ref: TaskRef) {
        self.clients
            .entry(client_id)
            .or_default()
            .error_tasks
            .push(task_ref);
    }

    pub fn notify_client_key_in_memory(&mut self, client_id: ClientId, task_ref: TaskRef) {
        self.clients
            .entry(client_id)
            .or_default()
            .in_memory_tasks
            .push(task_ref);
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

    let worker_ref = create_worker(&mut core_ref.get_mut(), queue_sender, msg.address);
    let worker_id = worker_ref.get().id;
    let mut notifications = Notifications::default();
    notifications.new_worker(&worker_ref.get());
    comm_ref
        .get_mut()
        .notify(&mut core_ref.get_mut(), notifications)?;

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
            comm_ref.get_mut().notify(&mut core, notifications).unwrap();
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
    client_key: DaskKey,
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
                        release_keys(&core_ref, &comm_ref, msg.client, msg.keys)?;
                    }
                    FromClientMessage::UpdateGraph(update) => {
                        update_graph(&core_ref, &comm_ref, client_id, update)?;
                    }
                    FromClientMessage::CloseClient => {
                        log::debug!("CloseClient message received");
                        break 'outer;
                    }
                    _ => panic!("Unhandled client message: {:?}", message),
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

pub async fn observe_scheduler(
    core_ref: CoreRef,
    comm_ref: CommRef,
    mut receiver: UnboundedReceiver<FromSchedulerMessage>,
) {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
            log::debug!("Scheduler registered: {:?}", r)
        }
        None => panic!("Scheduler closed connection without registration"),
        _ => panic!("First message of scheduler has to be registration"),
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut notifications = Default::default();
                core.process_assignments(assignments, &mut notifications);
                comm_ref.get_mut().notify(&mut core, notifications).unwrap();
            }
            FromSchedulerMessage::Register(_) => {
                panic!("Double registration of scheduler");
            }
        }
    }
}

/// Must be called within a LocalTaskSet
pub async fn connection_initiator(
    mut listener: TcpListener,
    core_ref: CoreRef,
    comm_ref: CommRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let core_ref = core_ref.clone();
        let comm_ref = comm_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            generic_rpc_loop(core_ref, comm_ref, socket, address)
                .await
                .expect("Connection failed");
            log::debug!("Connection ended: {}", address);
        });
    }
}

pub async fn generic_rpc_loop<T: AsyncRead + AsyncWrite>(
    core_ref: CoreRef,
    comm_ref: CommRef,
    stream: T,
    address: std::net::SocketAddr,
) -> crate::Result<()> {
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = dask_parse_stream::<GenericMessage, _>(asyncread_to_stream(reader));
    let mut writer = asyncwrite_to_sink(writer);

    'outer: while let Some(messages) = reader.next().await {
        for message in messages? {
            match message {
                GenericMessage::HeartbeatWorker(_) => {
                    log::debug!("Heartbeat from worker");
                }
                GenericMessage::RegisterWorker(msg) => {
                    log::debug!("Worker registration from {}", address);
                    let hb = RegisterWorkerResponseMsg {
                        status: to_dask_key("OK"),
                        time: 0.0,
                        heartbeat_interval: 1.0,
                        worker_plugins: Vec::new(),
                    };
                    writer.send(serialize_single_packet(hb)?).await?;
                    worker_rpc_loop(
                        &core_ref,
                        &comm_ref,
                        address,
                        dask_parse_stream(reader.into_inner()),
                        writer,
                        msg,
                    )
                    .await?;
                    break 'outer;
                }
                GenericMessage::RegisterClient(msg) => {
                    log::debug!("Client registration from {}", address);
                    let rsp = SimpleMessage {
                        op: to_dask_key("stream-start"),
                    };

                    // this has to be a list
                    writer.send(serialize_batch_packet(smallvec!(rsp))?).await?;

                    client_rpc_loop(
                        &core_ref,
                        &comm_ref,
                        address,
                        dask_parse_stream(reader.into_inner()),
                        writer,
                        msg.client,
                    )
                    .await?;
                    break 'outer;
                }
                GenericMessage::Identity(_) => {
                    log::debug!("Identity request from {}", address);
                    // TODO: get actual values
                    let rsp = IdentityResponse {
                        r#type: to_dask_key("Scheduler"),
                        id: core_ref.get().uid().into(),
                        workers: core_ref
                            .get()
                            .get_workers()
                            .iter()
                            .map(|w| {
                                let worker = w.get();
                                let address = worker.listen_address.clone();
                                (
                                    address.clone(),
                                    WorkerInfo {
                                        r#type: to_dask_key("worker"),
                                        host: address,
                                        id: worker.id.to_string().into(),
                                        last_seen: 0.0,
                                        local_directory: Default::default(),
                                        memory_limit: 0,
                                        metrics: Default::default(),
                                        name: to_dask_key(""),
                                        nanny: to_dask_key(""),
                                        nthreads: 0,
                                        resources: Default::default(),
                                        services: Default::default(),
                                    },
                                )
                            })
                            .collect(),
                    };
                    writer.send(serialize_single_packet(rsp)?).await?;
                }
                GenericMessage::WhoHas(msg) => {
                    log::debug!("WhoHas request from {} (keys={:?})", &address, msg.keys);
                    who_has(&core_ref, &comm_ref, &mut writer, msg.keys).await?;
                }
                GenericMessage::Gather(msg) => {
                    log::debug!("Gather request from {} (keys={:?})", &address, msg.keys);
                    gather(&core_ref, &comm_ref, address, &mut writer, msg.keys).await?;
                }
                GenericMessage::Scatter(msg) => {
                    log::debug!("Scatter request from {}", &address);
                    scatter(&core_ref, &comm_ref, &mut writer, msg).await?;
                }
                GenericMessage::Ncores => {
                    log::debug!("Ncores request from {}", &address);
                    get_ncores(&core_ref, &comm_ref, &mut writer).await?;
                }
                _ => panic!("Unhandled generic message: {:?}", message),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::comm::{generic_rpc_loop, Notifications};
    use crate::protocol::clientmsg::{
        ClientTaskSpec, FromClientMessage, KeyInMemoryMsg, ToClientMessage,
    };
    use crate::protocol::generic::{
        GenericMessage, IdentityMsg, IdentityResponse, RegisterClientMsg, RegisterWorkerMsg,
        SimpleMessage,
    };
    use crate::protocol::protocol::{
        serialize_single_packet, Batch, Frames, SerializedTransport,
    };
    use crate::protocol::workermsg::{FromWorkerMessage, RegisterWorkerResponseMsg};
    use crate::task::{DataInfo, TaskRuntimeState};
    use crate::test_util::{
        bytes_to_msg, client, dummy_address, dummy_ctx, dummy_serialized, frame, msg_to_bytes,
        packet_to_msg, packets_to_bytes, task_add, worker, MemoryStream,
    };
    use futures::StreamExt;
    use crate::protocol::key::to_dask_key;

    #[tokio::test]
    async fn respond_to_identity() -> crate::Result<()> {
        let msg = GenericMessage::<SerializedTransport>::Identity(IdentityMsg {});
        let (stream, msg_rx) = MemoryStream::new(msg_to_bytes(msg)?);
        let (core, comm, _rx) = dummy_ctx();
        generic_rpc_loop(core, comm, stream, dummy_address()).await?;
        let res: Batch<IdentityResponse> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].r#type.as_bytes(), b"Scheduler");

        Ok(())
    }

    #[tokio::test]
    async fn start_client() -> crate::Result<()> {
        let packets = vec![
            serialize_single_packet(GenericMessage::<SerializedTransport>::RegisterClient(
                RegisterClientMsg {
                    client: to_dask_key("test-client"),
                },
            ))?,
            serialize_single_packet(FromClientMessage::CloseClient::<SerializedTransport>)?,
        ];
        let (stream, msg_rx) = MemoryStream::new(packets_to_bytes(packets)?);
        let (core, comm, _rx) = dummy_ctx();
        generic_rpc_loop(core, comm, stream, dummy_address()).await?;
        let res: Batch<SimpleMessage> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].op.as_bytes(), b"stream-start");

        Ok(())
    }

    #[tokio::test]
    async fn start_worker() -> crate::Result<()> {
        let packets = vec![
            serialize_single_packet(GenericMessage::<SerializedTransport>::RegisterWorker(
                RegisterWorkerMsg {
                    address: to_dask_key("127.0.0.1"),
                },
            ))?,
            serialize_single_packet(FromWorkerMessage::<SerializedTransport>::Unregister)?,
        ];
        let (stream, msg_rx) = MemoryStream::new(packets_to_bytes(packets)?);
        let (core, comm, _rx) = dummy_ctx();
        generic_rpc_loop(core, comm, stream, dummy_address()).await?;
        let res: Batch<RegisterWorkerResponseMsg> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].status.as_bytes(), b"OK");

        Ok(())
    }

    #[tokio::test]
    async fn notifications_client_key_in_memory() -> crate::Result<()> {
        let (core, comm, _) = dummy_ctx();
        let (client, mut rx) = client(0);
        let id = client.id();
        core.get_mut().register_client(client);

        let t = task_add(&mut core.get_mut(), 0);
        let r#type = vec![1, 2, 3];

        t.get_mut().state = TaskRuntimeState::Finished(
            DataInfo {
                size: 0,
                r#type: r#type.clone(),
            },
            vec![],
        );
        let key = t.get().key.to_owned();

        let mut notifications = Notifications::default();
        notifications.notify_client_key_in_memory(id, t.clone());
        comm.get_mut().notify(&mut core.get_mut(), notifications)?;

        let msg: Batch<ToClientMessage> = packet_to_msg(rx.next().await.unwrap())?;
        assert_eq!(
            msg[0],
            ToClientMessage::KeyInMemory(KeyInMemoryMsg { key: key.into(), r#type })
        );

        Ok(())
    }

    #[tokio::test]
    async fn notifications_worker_compute_msg() -> crate::Result<()> {
        let (core, comm, _) = dummy_ctx();
        let (worker, mut rx) = worker(&mut core.get_mut(), "worker");

        let t = task_add(&mut core.get_mut(), 0);
        t.get_mut().spec = ClientTaskSpec::Direct {
            function: dummy_serialized(),
            args: dummy_serialized(),
            kwargs: Some(dummy_serialized()),
        };
        let mut notifications = Notifications::default();
        notifications.compute_task_on_worker(worker.clone(), t.clone());
        comm.get_mut().notify(&mut core.get_mut(), notifications)?;

        let packet = rx.next().await.unwrap();
        assert_eq!(packet.main_frame, frame(b"\x91\x86\xa2op\xaccompute-task\xa3key\xa10\xa8duration\xca?\0\0\0\xa8function\xc0\xa4args\xc0\xa6kwargs\xc0"));
        assert_eq!(packet.additional_frames, Frames::from(vec!()));

        Ok(())
    }
}
