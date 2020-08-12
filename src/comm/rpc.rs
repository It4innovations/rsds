use crate::comm::notifications::Notifications;
use crate::comm::reactor::{
    gather, get_ncores, proxy_to_worker, release_keys, scatter, subscribe_keys, update_graph,
    who_has,
};
use crate::comm::CommRef;
use crate::protocol::clientmsg::FromClientMessage;
use crate::protocol::generic::{
    GenericMessage, IdentityResponse, RegisterWorkerMsg, SimpleMessage, WorkerInfo,
};
use crate::protocol::key::{to_dask_key, DaskKey};
use crate::protocol::protocol::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, serialize_batch_packet,
    serialize_single_packet, Batch, DaskPacket,
};
use crate::protocol::workermsg::FromWorkerMessage;
use crate::protocol::workermsg::RegisterWorkerResponseMsg;
use crate::protocol::workermsg::Status;
use crate::server::client::Client;
use crate::server::core::CoreRef;

use crate::server::task::ErrorInfo;
use crate::server::worker::create_worker;

use crate::util::forward_queue_to_sink;
use futures::{FutureExt, Sink, SinkExt, StreamExt};
use smallvec::smallvec;
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::stream::Stream;

pub async fn worker_rpc_loop<
    Reader: Stream<Item = crate::Result<Batch<FromWorkerMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::Error> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    sender: Writer,
    msg: RegisterWorkerMsg,
) -> crate::Result<()> {
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let worker_ref = create_worker(
        &mut core_ref.get_mut(),
        queue_sender,
        msg.address,
        msg.nthreads,
    );
    let worker_id = worker_ref.get().id;
    let mut notifications = Notifications::default();
    notifications.new_worker(&worker_ref.get());
    comm_ref
        .get_mut()
        .notify(&mut core_ref.get_mut(), notifications)?;

    log::info!("Worker {} registered from {}", worker_id, address);

    let snd_loop = forward_queue_to_sink(queue_receiver, sender);

    let core_ref2 = core_ref.clone();
    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let mut notifications = Notifications::default();

            let messages = messages?;
            let mut core = core_ref.get_mut();
            for message in messages {
                log::debug!("Worker recv message {:?}", message);
                match message {
                    FromWorkerMessage::TaskFinished(msg) => {
                        assert_eq!(msg.status, Status::Ok); // TODO: handle other cases ??
                        core.on_task_finished(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::AddKeys(msg) => {
                        core.on_tasks_transferred(&worker_ref, msg.keys, &mut notifications);
                    }
                    FromWorkerMessage::TaskErred(msg) => {
                        assert_eq!(msg.status, Status::Error); // TODO: handle other cases ??
                        let error_info = ErrorInfo {
                            exception: msg.exception,
                            traceback: msg.traceback,
                        };
                        core.on_task_error(&worker_ref, msg.key, error_info, &mut notifications);
                        // TODO: Inform scheduler
                    }
                    FromWorkerMessage::StealResponse(msg) => {
                        core.on_steal_response(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::KeepAlive => { /* Do nothing by design */ }
                    FromWorkerMessage::Release(_) => { /* Do nothing TODO */ }
                    FromWorkerMessage::Unregister | FromWorkerMessage::CloseStream => break 'outer,
                }
            }
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
    Writer: Sink<DaskPacket, Error = crate::Error> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    sender: Writer,
    client_key: DaskKey,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, snd_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let client_id = {
        let mut core = core_ref.get_mut();
        let client_id = core.new_client_id();
        let client = Client::new(client_id, client_key, snd_sender);
        core.register_client(client);
        client_id
    };

    log::info!("Client {} registered from {}", client_id, address);

    let snd_loop = forward_queue_to_sink(snd_receiver, sender);
    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let messages = messages?;
            for message in messages {
                log::debug!("Client recv message");
                match message {
                    FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                    FromClientMessage::ClientReleasesKeys(msg) => {
                        release_keys(&core_ref, &comm_ref, msg.client, msg.keys)?;
                    }
                    FromClientMessage::ClientDesiresKeys(msg) => {
                        subscribe_keys(&core_ref, &comm_ref, msg.client, msg.keys)?;
                    }
                    FromClientMessage::UpdateGraph(update) => {
                        trace_time!("client", "update_graph", {
                            update_graph(&core_ref, &comm_ref, client_id, update)?;
                        });
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
    let address_str: &str = &address.to_string();

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
                        time: SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs_f64(),
                        heartbeat_interval: 1.0.into(),
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
                    trace_time!(address_str, "gather", {
                        gather(&core_ref, &comm_ref, address, &mut writer, msg.keys).await?;
                    });
                }
                GenericMessage::Scatter(msg) => {
                    log::debug!("Scatter request from {}", &address);
                    scatter(&core_ref, &comm_ref, &mut writer, msg).await?;
                }
                GenericMessage::Ncores => {
                    log::debug!("Ncores request from {}", &address);
                    get_ncores(&core_ref, &comm_ref, &mut writer).await?;
                }
                GenericMessage::Proxy(msg) => {
                    log::debug!("Proxy request from {}", &address);
                    proxy_to_worker(&core_ref, &comm_ref, &mut writer, msg).await?;
                }
                GenericMessage::Unregister => {
                    // TODO: remove worker
                    writer.send(serialize_single_packet("OK")?).await?;
                }
                _ => panic!("Unhandled generic message: {:?}", message),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::comm::notifications::Notifications;
    use crate::comm::rpc::generic_rpc_loop;
    use crate::protocol::clientmsg::{
        ClientTaskSpec, DirectTaskSpec, FromClientMessage, KeyInMemoryMsg, ToClientMessage,
    };
    use crate::protocol::generic::{
        GenericMessage, IdentityMsg, IdentityResponse, RegisterClientMsg, RegisterWorkerMsg,
        SimpleMessage,
    };
    use crate::protocol::key::{to_dask_key, DaskKey};
    use crate::protocol::protocol::{serialize_single_packet, Batch, Frames, SerializedTransport};
    use crate::protocol::workermsg::{FromWorkerMessage, RegisterWorkerResponseMsg};
    use crate::server::task::{DataInfo, TaskRuntimeState};
    use crate::test_util::{
        bytes_to_msg, client, dummy_address, dummy_ctx, dummy_serialized, frame, msg_to_bytes,
        packet_to_msg, packets_to_bytes, task_add, worker, MemoryStream,
    };
    use futures::StreamExt;

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
            serialize_single_packet(FromClientMessage::CloseClient)?,
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
                    name: "".to_string(),
                    address: to_dask_key("127.0.0.1"),
                    nthreads: 1,
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
            Default::default(),
        );
        let key: DaskKey = t.get().key().into();

        let mut notifications = Notifications::default();
        notifications.notify_client_key_in_memory(id, t.clone());
        comm.get_mut().notify(&mut core.get_mut(), notifications)?;

        let msg: Batch<ToClientMessage> = packet_to_msg(rx.next().await.unwrap())?;
        assert_eq!(
            msg[0],
            ToClientMessage::KeyInMemory(KeyInMemoryMsg { key, r#type })
        );

        Ok(())
    }

    #[tokio::test]
    async fn notifications_worker_compute_msg() -> crate::Result<()> {
        let (core, comm, _) = dummy_ctx();
        let (worker, mut rx) = worker(&mut core.get_mut(), "worker");

        let t = task_add(&mut core.get_mut(), 0);
        t.get_mut().spec = Some(ClientTaskSpec::Direct(DirectTaskSpec {
            function: Some(dummy_serialized()),
            args: Some(dummy_serialized()),
            kwargs: Some(dummy_serialized()),
        }));
        let mut notifications = Notifications::default();
        notifications.compute_task_on_worker(worker.clone(), t.clone());
        comm.get_mut().notify(&mut core.get_mut(), notifications)?;

        let packet = rx.next().await.unwrap();
        assert_eq!(packet.main_frame, frame(b"\x91\x87\xa2op\xaccompute-task\xa3key\xa10\xa8duration\xca?\0\0\0\xa8function\xc0\xa4args\xc0\xa6kwargs\xc0\xa8priority\x93\0\0\0"));
        assert_eq!(packet.additional_frames, Frames::from(vec!()));

        Ok(())
    }
}
