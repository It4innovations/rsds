use super::schedproto::*;
use super::state::State;
use futures;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::thread;
use tokio;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub type SchedulerComm = (
    UnboundedSender<ToSchedulerMessage>,
    UnboundedReceiver<FromSchedulerMessage>,
);

pub trait Scheduler {
    fn start(self) -> SchedulerComm;
}

pub struct BasicScheduler;

impl Scheduler for BasicScheduler {
    fn start(self) -> SchedulerComm {
        let (mut up_sender, mut up_receiver) = unbounded_channel::<ToSchedulerMessage>();
        let (mut down_sender, mut down_receiver) = unbounded_channel::<FromSchedulerMessage>();

        thread::spawn(move || {
            log::debug!("Scheduler initialized");
            let mut state = State::new();

            {
                let msg = FromSchedulerMessage::Register(SchedulerRegistration {
                    protocol_version: 0,
                    scheduler_name: "test_scheduler".into(),
                    scheduler_version: "0.0".into(),
                    reassigning: false,
                });
                down_sender.try_send(msg).unwrap();
            }
            let mut runtime = tokio::runtime::current_thread::Runtime::new().unwrap();
            runtime.block_on(up_receiver.for_each(move |msg| {
                log::debug!("Scheduler received message: {:?}", msg);
                match msg {
                    ToSchedulerMessage::Update(update) => {
                        state.update(update, &mut down_sender);
                    }
                };
                futures::future::ready(())
            }));
            log::debug!("Scheduler closed");
        });
        (up_sender, down_receiver)
    }
}
