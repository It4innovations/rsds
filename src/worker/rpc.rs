use std::cmp::Reverse;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::buf::BufMutExt;
use bytes::{Bytes, BytesMut};
use futures::Future;
use futures::{SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::lookup_host;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::LocalSet;
use tokio::time::delay_for;

use crate::common::rpc::forward_queue_to_sink;
use crate::common::transport::make_protocol_builder;
use crate::common::Map;
use crate::server::protocol::daskmessages::worker::{GetDataResponse, ToWorkerGenericMessage};
use crate::server::protocol::dasktransport::SerializedMemory::Indexed;
use crate::server::protocol::dasktransport::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, deserialize_packet,
    serialize_single_packet, Batch,
};
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::messages::generic::{GenericMessage, RegisterWorkerMsg};
use crate::server::protocol::messages::worker::ToWorkerMessage;
use crate::worker::reactor::try_start_tasks;
use crate::worker::state::WorkerStateRef;
use crate::worker::subworker::SubworkerRef;
use crate::worker::task::TaskRef;

async fn start_listener() -> crate::Result<(TcpListener, String)> {
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
    Ok((listener, address))
}

async fn connect_to_server(scheduler_address: &str) -> crate::Result<TcpStream> {
    let address = lookup_host(&scheduler_address)
        .await?
        .next()
        .expect("Invalid scheduler address");

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                return Ok(stream);
            }
            Err(e) => {
                log::error!("Could not connect to {}, error: {}", scheduler_address, e);
                delay_for(Duration::from_secs(2)).await;
            }
        }
    }
    Result::Err(crate::Error::GenericError(
        "Server could not be connected".into(),
    ))
}

pub async fn run_worker(
    scheduler_address: &str,
    ncpus: u32,
    subworkers: Vec<SubworkerRef>,
    sw_processes: impl Future<Output = usize>,
) -> crate::Result<()> {
    let (listener, address) = start_listener().await?;
    let stream = connect_to_server(&scheduler_address).await?;

    let (writer, reader) = make_protocol_builder().new_framed(stream).split();
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let taskset = LocalSet::default();
    {
        let message = GenericMessage::RegisterWorker(RegisterWorkerMsg {
            address: address.clone().into(),
            ncpus,
        });
        let mut frame = BytesMut::default().writer();
        rmp_serde::encode::write_named(&mut frame, &message)?;
        queue_sender.send(frame.into_inner().into()).unwrap();
    }

    let state = WorkerStateRef::new(queue_sender, ncpus, address, &subworkers);
    std::mem::forget(subworkers);

    tokio::select! {
        _ = worker_message_loop(state.clone(), reader) => {
            panic!("Connection to server lost");
        }
        _ = forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Cannot send a message to server");
        }
        result = taskset.run_until(connection_initiator(listener, state)) => {
            panic!("Taskset failed");
        }
        idx = sw_processes => {
            panic!("Subworker process {} failed", idx);
        }
    }
    Ok(())
}

async fn worker_message_loop(
    state_ref: WorkerStateRef,
    mut stream: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
) -> crate::Result<()> {
    while let Some(data) = stream.next().await {
        let data = data?;
        let message: ToWorkerMessage = rmp_serde::from_slice(&data)?;
        let mut state = state_ref.get_mut();
        match message {
            ToWorkerMessage::ComputeTask(msg) => {
                let task_ref = TaskRef::new(msg);
                let priority = task_ref.get().priority;
                state.task_queue.push(task_ref, Reverse(priority));
                try_start_tasks(&mut state);
            }
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc::UnboundedReceiver;

    use crate::server::protocol::daskmessages::generic::GenericMessage;
    use crate::server::protocol::daskmessages::worker::{
        ComputeTaskMsg, FromWorkerMessage, RegisterWorkerResponseMsg, TaskFinishedMsg,
        ToWorkerStreamMessage,
    };
    use crate::server::protocol::dasktransport::{
        serialize_single_packet, Batch, DaskPacket, SerializedTransport,
    };
    use crate::test_util::{bytes_to_msg, packets_to_bytes, MemoryStream};
    use crate::worker::rpc::main_rpc_loop;
    use crate::worker::state::WorkerStateRef;

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
        use crate::server::protocol::daskmessages::worker::Status;
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

    fn create_ctx(ncpus: u32, address: &str) -> (WorkerStateRef, UnboundedReceiver<Bytes>) {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        (
            WorkerStateRef::new(queue_sender, ncpus, address.to_string()),
            queue_receiver,
        )
    }
}
