use crate::trace::setup_file_trace;
use tokio::sync::mpsc::UnboundedReceiver;

#[macro_export]
macro_rules! from_dask_transport {
    (test, $ty:ty) => {
        #[cfg(test)]
        from_dask_transport!($ty);
    };
    ($ty:ty) => {
        impl $crate::server::protocol::dasktransport::FromDaskTransport for $ty {
            type Transport = Self;

            fn deserialize(
                source: Self::Transport,
                _frames: &mut $crate::server::protocol::dasktransport::Frames,
            ) -> Self {
                source
            }
        }
    };
}

pub fn setup_interrupt() -> UnboundedReceiver<()> {
    let (end_tx, end_rx) = tokio::sync::mpsc::unbounded_channel();
    ctrlc::set_handler(move || {
        log::debug!("Received SIGINT, attempting to stop");
        end_tx
            .send(())
            .unwrap_or_else(|_| log::error!("Sending signal failed"))
    })
    .expect("Error setting Ctrl-C handler");
    end_rx
}

pub fn setup_logging(trace_file: Option<String>) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::builder().format_timestamp_millis().init();

    if let Some(trace_file) = trace_file {
        setup_file_trace(trace_file);
    }
}
