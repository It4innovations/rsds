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
use std::path::Path;

use crate::common::rpc::forward_queue_to_sink;
use crate::common::transport::make_protocol_builder;
use crate::common::Map;
//use crate::server::protocol::daskmessages::worker::{GetDataResponse, ToWorkerGenericMessage};
use crate::server::protocol::dasktransport::SerializedMemory::Indexed;
use crate::server::protocol::dasktransport::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, deserialize_packet,
    serialize_single_packet, Batch,
};
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::messages::generic::{GenericMessage, RegisterWorkerMsg};
use crate::server::protocol::messages::worker::{ToWorkerMessage, FetchRequest, FetchResponse, FetchResponseData};
use crate::worker::reactor::{try_start_tasks};
use crate::worker::state::WorkerStateRef;
use crate::worker::subworker::{SubworkerRef, start_subworkers};
use crate::worker::task::TaskRef;
use crate::worker::data::{DataObjectRef, DataObjectState, RemoteData};
use crate::server::protocol::PriorityValue;
use crate::server::protocol::Priority;
use tracing_subscriber::registry::Data;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use crate::server::reactor::fetch_data;


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
    log::info!("Connecting to server {}", scheduler_address);
    let address = lookup_host(&scheduler_address)
        .await?
        .next()
        .expect("Invalid scheduler address");

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                log::debug!("Connected to server");
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
    work_dir: &Path
) -> crate::Result<()> {
    let (listener, address) = start_listener().await?;
    let stream = connect_to_server(&scheduler_address).await?;
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let (download_sender, download_reader) = tokio::sync::mpsc::unbounded_channel::<(DataObjectRef, (PriorityValue, PriorityValue))>();

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

    let state = WorkerStateRef::new(queue_sender, ncpus, address, download_sender);

    log::info!("Starting {} subworkers", ncpus);
    let (subworkers, sw_processes) = start_subworkers(&state, &work_dir, "python3", ncpus).await?;
    log::debug!("Subworkers started");

    state.get_mut().set_subworkers(subworkers);
    let (writer, reader) = make_protocol_builder().new_framed(stream).split();

    tokio::select! {
        _ = worker_message_loop(state.clone(), reader) => {
            panic!("Connection to server lost");
        }
        _ = forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Cannot send a message to server");
        }
        result = taskset.run_until(connection_initiator(listener, state.clone())) => {
            panic!("Taskset failed");
        }
        idx = sw_processes => {
            panic!("Subworker process {} failed", idx);
        }
        _ = worker_data_downloader(state, download_reader) => {
            unreachable!()
        }
    }
    Ok(())
}


async fn worker_data_downloader(state_ref: WorkerStateRef, mut stream: tokio::sync::mpsc::UnboundedReceiver<(DataObjectRef, Priority)>) {
    // TODO: Limit downloads, more parallel downloads, respect priorities
    // TODO: Reuse connections
    let queue : priority_queue::PriorityQueue<DataObjectRef, Reverse<Priority>> = Default::default();
    let mut random = SmallRng::from_entropy();
    loop {
        let (data_ref, priority) = stream.next().await.unwrap();

        let (worker_address, key) = {
            let data_obj = data_ref.get();
            let workers = match &data_obj.state {
                DataObjectState::Remote(rs) => {
                    &rs.workers
                },
                DataObjectState::Local(_) | DataObjectState::Removed => {
                    /* It is already finished */
                    continue;
                },
            };
            let worker_address = workers.choose(&mut random).cloned().unwrap();
            (worker_address, data_obj.key.clone())
        };

        let mut connection = connect_to_worker(worker_address).await.unwrap();
        let mut stream  = make_protocol_builder().new_framed(connection);
        let (data, serializer) = fetch_data(&mut stream, key).await.unwrap();
        state_ref.get_mut().on_data_downloaded(data_ref, data, serializer);
    }
}

/* TODO: Refactor this! the same function is in the server */
async fn connect_to_worker(address: DaskKey) -> crate::Result<tokio::net::TcpStream> {
    let address = address.to_string();
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
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
            ToWorkerMessage::ComputeTask(mut msg) => {
                log::debug!("Task assigned: {}", msg.key);
                let dep_info = std::mem::take(&mut msg.dep_info);
                let task_ref = TaskRef::new(msg);
                for (key, size, workers) in dep_info {
                    state.add_dependancy(&task_ref, key.clone(), size, workers);
                }
                state.add_task(task_ref);
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

async fn connection_rpc_loop(
    stream: TcpStream,
    state_ref: WorkerStateRef,
    address: SocketAddr,
) -> crate::Result<()> {
    let mut stream = make_protocol_builder().new_framed(stream);
    loop {
        let data = match stream.next().await {
            None => return Ok(()),
            Some(data) => data?
        };
        let request: FetchRequest = rmp_serde::from_slice(&data)?;
        log::debug!("Object {} request from {} started", request.key, address);
        let state = state_ref.get();
        let (bytes, serializer) = {
            let data_obj = match state.data_objects.get(&request.key) {
                None => {
                    let response = FetchResponse::NotAvailable;
                    let data = rmp_serde::to_vec_named(&response).unwrap();
                    stream.send(data.into()).await?;
                    continue;
                }
                Some(x) => x.get()
            };
            let data = data_obj.local_data().unwrap();
            (data.bytes.clone(), data.serializer.clone())
        };
        let response = FetchResponse::Data(FetchResponseData {
                serializer: serializer,
        });
        let data = rmp_serde::to_vec_named(&response).unwrap();
        stream.send(data.into()).await?;
        stream.send(bytes).await?;
        log::debug!("Object {} request from {} finished", request.key, address);
    }
}
