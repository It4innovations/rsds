use futures::SinkExt;
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};

use tokio::net::TcpListener;

use smallvec::smallvec;

use crate::core::CoreRef;
use crate::reactor::{gather, get_ncores, scatter, who_has, ReactorRef};

use crate::client::execute_client;
use crate::protocol::generic::{GenericMessage, IdentityResponse, SimpleMessage, WorkerInfo};
use crate::protocol::protocol::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, serialize_batch_packet,
    serialize_single_packet,
};
use crate::protocol::workermsg::RegisterWorkerResponseMsg;
use crate::worker::execute_worker;

/// Must be called within a LocalTaskSet
pub async fn connection_initiator(
    mut listener: TcpListener,
    core_ref: CoreRef,
    reactor_ref: ReactorRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let core_ref = core_ref.clone();
        let reactor_ref = reactor_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            handle_connection(core_ref, reactor_ref, socket, address)
                .await
                .expect("Connection failed");
            log::debug!("Connection ended: {}", address);
        });
    }
}

pub async fn handle_connection<T: AsyncRead + AsyncWrite>(
    core_ref: CoreRef,
    reactor_ref: ReactorRef,
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
                        status: "OK".to_owned(),
                        time: 0.0,
                        heartbeat_interval: 1.0,
                        worker_plugins: Vec::new(),
                    };
                    writer.send(serialize_single_packet(hb)?).await?;
                    execute_worker(
                        &core_ref,
                        &reactor_ref,
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
                        op: "stream-start".to_owned(),
                    };

                    // this has to be a list
                    writer.send(serialize_batch_packet(smallvec!(rsp))?).await?;

                    execute_client(
                        &core_ref,
                        &reactor_ref,
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
                        r#type: "Scheduler".to_owned(),
                        id: core_ref.get().uid().to_owned(),
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
                                        r#type: "worker".to_string(),
                                        host: address,
                                        id: worker.id.to_string(),
                                        last_seen: 0.0,
                                        local_directory: Default::default(),
                                        memory_limit: 0,
                                        metrics: Default::default(),
                                        name: "".to_string(),
                                        nanny: "".to_string(),
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
                    who_has(&core_ref, &reactor_ref, &mut writer, msg.keys).await?;
                }
                GenericMessage::Gather(msg) => {
                    log::debug!("Gather request from {} (keys={:?})", &address, msg.keys);
                    gather(&core_ref, &reactor_ref, address, &mut writer, msg.keys).await?;
                }
                GenericMessage::Scatter(msg) => {
                    log::debug!("Scatter request from {}", &address);
                    scatter(&core_ref, &reactor_ref, &mut writer, msg).await?;
                }
                GenericMessage::Ncores => {
                    log::debug!("Ncores request from {}", &address);
                    get_ncores(&core_ref, &reactor_ref, &mut writer).await?;
                }
                _ => panic!("Unhandled generic message: {:?}", message),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::connection::handle_connection;
    use crate::protocol::clientmsg::FromClientMessage;
    use crate::protocol::generic::{
        GenericMessage, IdentityMsg, IdentityResponse, RegisterClientMsg, RegisterWorkerMsg,
        SimpleMessage,
    };
    use crate::protocol::protocol::{serialize_single_packet, Batch, SerializedTransport};
    use crate::protocol::workermsg::{FromWorkerMessage, RegisterWorkerResponseMsg};
    use crate::test_util::{
        bytes_to_msg, dummy_address, dummy_core, msg_to_bytes, packets_to_bytes, MemoryStream,
    };

    #[tokio::test]
    async fn respond_to_identity() -> crate::Result<()> {
        let msg = GenericMessage::<SerializedTransport>::Identity(IdentityMsg {});
        let (stream, msg_rx) = MemoryStream::new(msg_to_bytes(msg)?);
        let (core, _rx) = dummy_core();
        handle_connection(core, stream, dummy_address()).await?;
        let res: Batch<IdentityResponse> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].r#type, "Scheduler");

        Ok(())
    }

    #[tokio::test]
    async fn start_client() -> crate::Result<()> {
        let packets = vec![
            serialize_single_packet(GenericMessage::<SerializedTransport>::RegisterClient(
                RegisterClientMsg {
                    client: "test-client".to_string(),
                },
            ))?,
            serialize_single_packet(FromClientMessage::CloseClient::<SerializedTransport>)?,
        ];
        let (stream, msg_rx) = MemoryStream::new(packets_to_bytes(packets)?);
        let (core, _rx) = dummy_core();
        handle_connection(core, stream, dummy_address()).await?;
        let res: Batch<SimpleMessage> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].op, "stream-start".to_owned());

        Ok(())
    }

    #[tokio::test]
    async fn start_worker() -> crate::Result<()> {
        let packets = vec![
            serialize_single_packet(GenericMessage::<SerializedTransport>::RegisterWorker(
                RegisterWorkerMsg {
                    address: "127.0.0.1".to_string(),
                },
            ))?,
            serialize_single_packet(FromWorkerMessage::<SerializedTransport>::Unregister)?,
        ];
        let (stream, msg_rx) = MemoryStream::new(packets_to_bytes(packets)?);
        let (core, _rx) = dummy_core();
        handle_connection(core, stream, dummy_address()).await?;
        let res: Batch<RegisterWorkerResponseMsg> = bytes_to_msg(&msg_rx.get())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].status, "OK".to_owned());

        Ok(())
    }
}
