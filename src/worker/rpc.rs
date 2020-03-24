use crate::common::Map;
use crate::protocol::generic::{GenericMessage, RegisterWorkerMsg};
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::SerializedMemory::Indexed;
use crate::protocol::protocol::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, deserialize_packet,
    serialize_single_packet, Batch, DaskPacket, SerializedTransport,
};
use crate::protocol::workermsg::{
    GetDataResponse, RegisterWorkerResponseMsg, ToWorkerGenericMessage, ToWorkerStreamMessage,
};
use crate::util::forward_queue_to_sink;
use crate::worker::reactor::compute_task;
use crate::worker::state::WorkerStateRef;
use bytes::BytesMut;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::LocalSet;

pub async fn run_worker<R: AsyncRead + AsyncWrite>(server_stream: R) -> crate::Result<()> {
    let taskset = LocalSet::default();

    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(address).await?;
    let address = {
        let socketaddr = listener.local_addr()?;
        format!(
            "{}:{}",
            gethostname::gethostname().into_string().unwrap(),
            socketaddr.port()
        )
    };
    log::info!("Listening on {}", address);

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();
    let ncpus = 1; // TODO
    let state = WorkerStateRef::new(queue_sender, ncpus, address);

    taskset
        .run_until(async move {
            let main_loop =
                main_rpc_loop(server_stream, state.clone(), queue_receiver).boxed_local();
            let initiator = connection_initiator(listener, state).boxed_local();

            futures::future::select(main_loop, initiator)
                .await
                .factor_first()
        })
        .await
        .0
}

pub async fn connection_initiator(
    mut listener: TcpListener,
    state_ref: WorkerStateRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;

        let state = state_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            connection_rpc_loop(socket, state, address)
                .await
                .expect("Connection failed");
            log::debug!("Connection ended: {}", address);
        });
    }
}

async fn connection_rpc_loop<R: AsyncRead + AsyncWrite>(
    stream: R,
    state_ref: WorkerStateRef,
    address: SocketAddr,
) -> crate::Result<()> {
    let (reader, writer) = tokio::io::split(stream);
    let reader = asyncread_to_stream(reader);
    let mut writer = asyncwrite_to_sink(writer);

    let mut reader = dask_parse_stream::<ToWorkerGenericMessage, _>(reader);
    while let Some(messages) = reader.next().await {
        for message in messages? {
            match message {
                ToWorkerGenericMessage::GetData(request) => {
                    let data: Map<_, _> = request
                        .keys
                        .into_iter()
                        .map(|key| {
                            (
                                key,
                                Indexed {
                                    frames: vec![BytesMut::from(&b"\x80\x03K*."[..])],
                                    header: rmpv::Value::Map(vec![
                                        (
                                            rmpv::Value::String("serializer".into()),
                                            rmpv::Value::String("pickle".into()),
                                        ),
                                        (
                                            rmpv::Value::String("lengths".into()),
                                            rmpv::Value::Array(vec![5.into()]),
                                        ),
                                        (rmpv::Value::String("count".into()), 1.into()),
                                        (rmpv::Value::String("deserialize".into()), false.into()),
                                    ]),
                                },
                            )
                        })
                        .collect();
                    let response = GetDataResponse {
                        status: "OK".into(),
                        data,
                    };
                    writer
                        .send(serialize_single_packet(response).unwrap())
                        .await
                        .unwrap();
                    let mut inner = reader.into_inner();
                    let packet = inner.next().await.unwrap()?;
                    let response: Batch<DaskKey> = deserialize_packet(packet)?;
                    assert_eq!(response.len(), 1);
                    assert_eq!(response[0], "OK".into());
                    reader = dask_parse_stream::<ToWorkerGenericMessage, _>(inner);
                }
                _ => {
                    log::warn!("Unhandled message: {:?}", message);
                }
            };
            //Indexed { frames: [b"\x80\x04\x95\x03\0\0\0\0\0\0\0K\0."], header: Map([
            // (String(Utf8String { s: Ok("serializer") }), String(Utf8String { s: Ok("pickle") })), (String(Utf8String { s: Ok("lengths") }), Array([Integer(PosInt(14))])), (String(Utf8String { s: Ok("compression") }), Array([Nil])), (String(Utf8String { s: Ok("count") }), Integer(PosInt(1))), (String(Utf8String { s: Ok("deserialize") }), Boolean(false))]) }}
        }
    }

    Ok(())
}

async fn worker_rpc_loop<S: Stream<Item = crate::Result<Batch<ToWorkerStreamMessage>>> + Unpin>(
    mut stream: S,
    state_ref: WorkerStateRef,
) -> crate::Result<()> {
    while let Some(messages) = stream.next().await {
        for message in messages? {
            match message {
                ToWorkerStreamMessage::ComputeTask(msg) => {
                    compute_task(&state_ref, msg)?;
                }
                _ => {
                    log::warn!("Unhandled worker message: {:?}", message);
                }
            }
        }
    }
    Ok(())
}

async fn main_rpc_loop<R: AsyncRead + AsyncWrite>(
    stream: R,
    state_ref: WorkerStateRef,
    queue_receiver: UnboundedReceiver<DaskPacket>,
) -> crate::Result<()> {
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = dask_parse_stream::<RegisterWorkerResponseMsg, _>(asyncread_to_stream(reader));
    let mut writer = asyncwrite_to_sink(writer);

    register_worker(state_ref.clone(), &mut reader, &mut writer).await?;

    let snd_loop = forward_queue_to_sink(queue_receiver, writer);
    let recv_loop = worker_rpc_loop(
        dask_parse_stream::<ToWorkerStreamMessage, _>(reader.into_inner()),
        state_ref,
    );

    let result = futures::future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
        panic!("Worker error: {}", e);
    }
    Ok(())
}

async fn register_worker<
    S: Stream<Item = crate::Result<Batch<RegisterWorkerResponseMsg>>> + Unpin,
    W: Sink<DaskPacket, Error = crate::Error> + Unpin,
>(
    state: WorkerStateRef,
    reader: &mut S,
    writer: &mut W,
) -> crate::Result<()> {
    let address = state.get().listen_address.clone();
    writer
        .send(serialize_single_packet(GenericMessage::<
            SerializedTransport,
        >::RegisterWorker(
            RegisterWorkerMsg {
                name: address.clone(),
                address: address.into(),
                nthreads: state.get().ncpus,
            },
        ))?)
        .await?;
    let response: Batch<RegisterWorkerResponseMsg> = reader
        .next()
        .await
        .expect("Didn't receive registration confirmation")?;
    assert_eq!(response.len(), 1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::protocol::generic::GenericMessage;
    use crate::protocol::protocol::{
        serialize_single_packet, Batch, DaskPacket, SerializedTransport,
    };
    use crate::protocol::workermsg::{
        ComputeTaskMsg, FromWorkerMessage, RegisterWorkerResponseMsg, TaskFinishedMsg,
        ToWorkerStreamMessage,
    };
    use crate::test_util::{bytes_to_msg, packets_to_bytes, MemoryStream};
    use crate::worker::rpc::main_rpc_loop;
    use crate::worker::state::WorkerStateRef;
    use tokio::sync::mpsc::UnboundedReceiver;

    #[tokio::test]
    async fn register_self() -> crate::Result<()> {
        let (state, rx) = create_ctx(2, "xxx:1234");
        let (stream, output) = MemoryStream::new(packets_to_bytes(vec![serialize_single_packet(
            RegisterWorkerResponseMsg::default(),
        )?])?);
        main_rpc_loop(stream, state, rx).await?;

        let mut response: Batch<GenericMessage> = bytes_to_msg(output.get().as_slice())?;
        assert_eq!(response.len(), 1);
        match response.pop().unwrap() {
            GenericMessage::RegisterWorker(msg) => {
                assert_eq!(msg.address, "xxx:1234".into());
                assert_eq!(msg.nthreads, 2);
            }
            _ => panic!("Wrong response"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn receive_compute_msg() -> crate::Result<()> {
        let (state, rx) = create_ctx(2, "xxx:1234");
        let (stream, output) = MemoryStream::new(packets_to_bytes(vec![
            serialize_single_packet(RegisterWorkerResponseMsg::default())?,
            serialize_single_packet(ToWorkerStreamMessage::ComputeTask(ComputeTaskMsg {
                key: "test".into(),
                duration: 0.0,
                actor: false,
                who_has: vec![],
                nbytes: vec![],
                function: Some(SerializedTransport::Inline(rmpv::Value::Binary(vec![]))),
                args: None,
                kwargs: None,
                task: None,
                priority: [0, 0, 0],
            }))?,
        ])?);
        main_rpc_loop(stream, state, rx).await?;

        Ok(())
    }

    #[test]
    fn test_serialize_task_finished() {
        use crate::protocol::workermsg::Status;
        let msg = TaskFinishedMsg {
            status: Status::Ok,
            key: Default::default(),
            nbytes: 0,
            r#type: vec![],
            startstops: vec![],
        };
        let msg = FromWorkerMessage::<SerializedTransport>::TaskFinished(msg);
        let v = rmp_serde::to_vec_named(&msg).unwrap();
        let r: FromWorkerMessage<SerializedTransport> = rmp_serde::from_slice(&v).unwrap();
    }

    fn create_ctx(ncpus: u32, address: &str) -> (WorkerStateRef, UnboundedReceiver<DaskPacket>) {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();
        (
            WorkerStateRef::new(queue_sender, ncpus, address.to_string()),
            queue_receiver,
        )
    }
}
