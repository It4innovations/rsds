use std::net::{Ipv4Addr, SocketAddr};
use std::{fmt, thread};

use futures::{FutureExt, StreamExt};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;

use rsds::comm::CommRef;
use rsds::core::CoreRef;
use rsds::scheduler::{observe_scheduler, prepare_scheduler_comm, scheduler_driver, SchedulerComm};
use serde::export::fmt::Arguments;
use std::fs::File;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing_subscriber::{fmt::time::FormatTime, FmtSubscriber};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn create_scheduler(
    r#type: SchedulerType,
    comm: SchedulerComm,
) -> Pin<Box<dyn Future<Output = rsds::Result<()>>>> {
    match r#type {
        SchedulerType::Workstealing => Box::pin(scheduler_driver(
            rsds::scheduler::WorkstealingScheduler::new(),
            comm,
        )),
        SchedulerType::Random => Box::pin(scheduler_driver(
            rsds::scheduler::RandomScheduler::default(),
            comm,
        )),
        SchedulerType::Blevel => Box::pin(scheduler_driver(
            rsds::scheduler::BlevelScheduler::default(),
            comm,
        )),
    }
}

#[derive(Debug)]
enum SchedulerType {
    Workstealing,
    Random,
    Blevel,
}

impl FromStr for SchedulerType {
    type Err = String;
    fn from_str(scheduler: &str) -> Result<Self, Self::Err> {
        match scheduler {
            "workstealing" => Ok(SchedulerType::Workstealing),
            "random" => Ok(SchedulerType::Random),
            "blevel" => Ok(SchedulerType::Blevel),
            _ => Err(format!("Scheduler '{}' does not exist", scheduler)),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds", about = "Rust Dask Scheduler")]
struct Opt {
    #[structopt(long, default_value = "8786")]
    port: u16,
    #[structopt(long, default_value = "workstealing")]
    scheduler: SchedulerType,
    #[structopt(long)]
    trace_file: Option<String>,
}

fn setup_interrupt() -> UnboundedReceiver<()> {
    let (end_tx, end_rx) = tokio::sync::mpsc::unbounded_channel();
    ctrlc::set_handler(move || {
        log::debug!("Received SIGINT, attempting to stop server");
        end_tx
            .send(())
            .unwrap_or_else(|_| log::error!("Sending signal failed"))
    })
    .expect("Error setting Ctrl-C handler");
    end_rx
}

fn setup_logging(trace_file: Option<String>) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::builder().format_timestamp_millis().init();

    if let Some(trace_file) = trace_file {
        struct Timestamp;
        impl FormatTime for Timestamp {
            fn format_time(&self, w: &mut dyn fmt::Write) -> fmt::Result {
                write!(
                    w,
                    "{}",
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                )
            }
        }

        struct FileGuard(Arc<Mutex<std::fs::File>>);
        impl std::io::Write for FileGuard {
            #[inline]
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().write(buf)
            }
            #[inline]
            fn flush(&mut self) -> std::io::Result<()> {
                self.0.lock().unwrap().flush()
            }
            #[inline]
            fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
                self.0.lock().unwrap().write_all(buf)
            }
            #[inline]
            fn write_fmt(&mut self, fmt: Arguments<'_>) -> std::io::Result<()> {
                self.0.lock().unwrap().write_fmt(fmt)
            }
        }

        let file = File::create(&trace_file).expect("Unable to create trace file");
        let file = Arc::new(Mutex::new(file));

        log::info!(
            "Writing trace to {}",
            std::path::PathBuf::from(trace_file)
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        );

        let make_writer = move || FileGuard(file.clone());

        let subscriber = FmtSubscriber::builder()
            .with_writer(make_writer)
            .json()
            .with_target(false)
            .with_ansi(false)
            .with_timer(Timestamp)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set global tracing subscriber");
    }
}

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();

    log::info!("rsds v0.1 started: {:?}", opt);

    setup_logging(opt.trace_file.take());
    let mut end_rx = setup_interrupt();

    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opt.port);
    log::info!("Listening on port {}", address);
    let listener = TcpListener::bind(address).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let scheduler_thread = thread::spawn(move || {
        let mut runtime = tokio::runtime::Builder::new()
            .basic_scheduler()
            .build()
            .expect("Runtime creation failed");
        runtime
            .block_on(create_scheduler(opt.scheduler, comm))
            .expect("Scheduler failed");
    });

    {
        let task_set = tokio::task::LocalSet::new();
        let comm_ref = CommRef::new(sender);
        let core_ref = CoreRef::new();
        let core_ref2 = core_ref.clone();
        let comm_ref2 = comm_ref.clone();
        task_set
            .run_until(async move {
                let scheduler = observe_scheduler(core_ref2, comm_ref2, receiver);
                let connection = rsds::comm::connection_initiator(listener, core_ref, comm_ref);
                let end_flag = async move {
                    end_rx.next().await;
                    Ok(())
                };

                let futures = vec![
                    scheduler.boxed_local(),
                    connection.boxed_local(),
                    end_flag.boxed_local(),
                ];
                let (res, _, _) = futures::future::select_all(futures).await;
                res
            })
            .await
            .expect("Rsds failed");
    }

    log::debug!("Waiting for scheduler to shut down...");
    scheduler_thread.join().expect("Scheduler thread failed");
    log::info!("rsds ends");

    Ok(())
}
