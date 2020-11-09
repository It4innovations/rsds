use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::{Sink, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use crate::common::rpc::forward_queue_to_sink;
use crate::common::transport::make_protocol_builder;
use crate::server::comm::CommRef;
use crate::server::core::CoreRef;
use crate::server::notifications::Notifications;
use crate::server::protocol::messages::generic::{GenericMessage, RegisterWorkerMsg};
use crate::server::protocol::messages::worker::FromWorkerMessage;
use crate::server::task::ErrorInfo;
use crate::server::worker::WorkerRef;

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
    let (writer, mut reader) = make_protocol_builder().new_framed(stream).split();
    while let Some(message_data) = reader.next().await {
        let message: GenericMessage = rmp_serde::from_slice(&message_data?)?;
        match message {
            GenericMessage::RegisterWorker(msg) => {
                log::debug!("Worker registration from {}", address);
                worker_rpc_loop(&core_ref, &comm_ref, address, reader, writer, msg).await?;
                break;
            }
        }
    }
    Ok(())
}

pub async fn worker_rpc_loop<
    Reader: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    Writer: Sink<Bytes, Error = std::io::Error> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    sender: Writer,
    msg: RegisterWorkerMsg,
) -> crate::Result<()> {
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let (worker_ref, worker_id) = {
        let mut core = core_ref.get_mut();
        let worker_id = core.new_worker_id();
        let worker_ref = WorkerRef::new(worker_id, msg.ncpus, queue_sender, msg.address);
        core.register_worker(worker_ref.clone());
        (worker_ref, worker_id)
    };

    let mut notifications = Notifications::default();
    notifications.new_worker(&worker_ref.get());
    comm_ref
        .get_mut()
        .notify(&mut core_ref.get_mut(), notifications)?;

    log::info!("Worker {} registered from {}", worker_id, address);

    let snd_loop = forward_queue_to_sink(
        queue_receiver,
        sender, /*.with(|msg| match msg {
                    WorkerMessage::Rsds(data) => futures::future::ok::<_, crate::Error>(data),
                    _ => panic!("Received Dask worker packet instead of RSDS worker packet"),
                })*/
    );

    let core_ref2 = core_ref.clone();
    let recv_loop = async move {
        while let Some(message) = receiver.next().await {
            // TODO: If more worker messages are waiting, process them at once and
            // after that send the notifications
            let message: FromWorkerMessage = rmp_serde::from_slice(&message.unwrap()).unwrap();
            let mut notifications = Notifications::default();
            let mut core = core_ref.get_mut();
            match message {
                FromWorkerMessage::TaskFinished(msg) => {
                    core.on_task_finished(&worker_ref, msg, &mut notifications);
                }
                FromWorkerMessage::TaskFailed(msg) => {
                    core.on_task_error(
                        &worker_ref,
                        msg.key,
                        ErrorInfo {
                            exception: msg.exception,
                            traceback: msg.traceback,
                        },
                        &mut notifications,
                    );
                }
                FromWorkerMessage::DataDownloaded(msg) => {
                    core.on_tasks_transferred(&worker_ref, &msg.key, &mut notifications)
                }
                FromWorkerMessage::StealResponse(msg) => {
                    core.on_steal_response(&worker_ref, msg, &mut notifications)
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
