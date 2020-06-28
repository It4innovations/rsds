use futures::{Sink, SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedReceiver;

#[macro_export]
macro_rules! from_dask_transport {
    (test, $ty:ty) => {
        #[cfg(test)]
        from_dask_transport!($ty);
    };
    ($ty:ty) => {
        impl $crate::protocol::protocol::FromDaskTransport for $ty {
            type Transport = Self;

            fn deserialize(
                source: Self::Transport,
                _frames: &mut $crate::protocol::protocol::Frames,
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

pub async fn forward_queue_to_sink<T, E, S: Sink<T, Error = E> + Unpin>(
    mut queue: UnboundedReceiver<T>,
    mut sink: S,
) -> Result<(), E> {
    while let Some(data) = queue.next().await {
        if let Err(e) = sink.send(data).await {
            log::error!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}
