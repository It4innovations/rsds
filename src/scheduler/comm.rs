use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::comm::CommRef;
use crate::scheduler::{FromSchedulerMessage, Scheduler, SchedulerSender, ToSchedulerMessage};
use crate::server::core::CoreRef;
use crate::Error;
use futures::future::Either;
use futures::{future, StreamExt};
use std::time::{Duration, Instant};
use tokio::time::delay_for;

/// Communication channels used by the scheduler to receive events and send assignments.
pub struct SchedulerComm {
    pub(crate) recv: UnboundedReceiver<Vec<ToSchedulerMessage>>,
    send: UnboundedSender<FromSchedulerMessage>,
}

impl SchedulerComm {
    pub fn send(&mut self, message: FromSchedulerMessage) {
        self.send
            .send(message)
            .expect("Couldn't send scheduler message")
    }
}

pub fn prepare_scheduler_comm() -> (
    SchedulerComm,
    UnboundedSender<Vec<ToSchedulerMessage>>,
    UnboundedReceiver<FromSchedulerMessage>,
) {
    let (up_sender, up_receiver) = unbounded_channel::<Vec<ToSchedulerMessage>>();
    let (down_sender, down_receiver) = unbounded_channel::<FromSchedulerMessage>();

    (
        SchedulerComm {
            recv: up_receiver,
            send: down_sender,
        },
        up_sender,
        down_receiver,
    )
}

pub async fn observe_scheduler(
    core_ref: CoreRef,
    comm_ref: CommRef,
    mut receiver: UnboundedReceiver<FromSchedulerMessage>,
) -> crate::Result<()> {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
            log::debug!("Scheduler registered: {:?}", r)
        }
        None => {
            return Err(Error::SchedulerError(
                "Scheduler closed connection without registration".to_owned(),
            ))
        }
        _ => {
            return Err(Error::SchedulerError(
                "First message of scheduler has to be registration".to_owned(),
            ))
        }
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut notifications = Default::default();

                trace_time!("core", "process_assignments", {
                    core.process_assignments(assignments, &mut notifications);
                    trace_time!("core", "notify", {
                        comm_ref.get_mut().notify(&mut core, notifications).unwrap();
                    });
                });
            }
            FromSchedulerMessage::Register(_) => {
                return Err(Error::SchedulerError(
                    "Double registration of scheduler".to_owned(),
                ));
            }
        }
    }

    Ok(())
}

pub async fn drive_scheduler<S: Scheduler>(
    mut scheduler: S,
    comm: SchedulerComm,
    minimum_delay: Duration,
) -> crate::Result<()> {
    let identity = scheduler.identify();
    let name = identity.scheduler_name.clone();

    log::debug!("Scheduler {} initialized", name);

    let SchedulerComm {
        send: mut sender,
        recv: mut receiver,
    } = comm;
    sender
        .send(FromSchedulerMessage::Register(identity))
        .unwrap();

    let mut last_schedule = Instant::now() - minimum_delay * 2;

    let run_schedule =
        |scheduler: &mut S, sender: &mut SchedulerSender, last_schedule: &mut Instant| {
            let assignments = trace_time!("scheduler", "schedule", scheduler.schedule());
            *last_schedule = Instant::now();
            sender
                .send(FromSchedulerMessage::TaskAssignments(assignments))
                .expect("Couldn't send scheduler assignments");
        };

    let mut recv_fut = receiver.next();
    let mut delay_fut = None;

    let needs_schedule = loop {
        match delay_fut {
            Some(delay) => match future::select(recv_fut, delay).await {
                Either::Left((messages, previous_delay)) => match messages {
                    Some(messages) => {
                        trace_time!("scheduler", "handle_messages", {
                            scheduler.handle_messages(messages)
                        });

                        delay_fut = Some(previous_delay);
                        recv_fut = receiver.next();
                    }
                    None => break true,
                },
                Either::Right((_, previous_recv)) => {
                    run_schedule(&mut scheduler, &mut sender, &mut last_schedule);
                    recv_fut = previous_recv;
                    delay_fut = None;
                }
            },
            None => match recv_fut.await {
                Some(messages) => {
                    let needs_schedule = trace_time!("scheduler", "handle_messages", {
                        scheduler.handle_messages(messages)
                    });

                    if needs_schedule {
                        let since_last_schedule = last_schedule.elapsed();
                        if since_last_schedule >= minimum_delay {
                            run_schedule(&mut scheduler, &mut sender, &mut last_schedule);
                        } else {
                            delay_fut = Some(delay_for(minimum_delay - since_last_schedule));
                        }
                    }
                    recv_fut = receiver.next();
                }
                None => break false,
            },
        }
    };

    if needs_schedule {
        run_schedule(&mut scheduler, &mut sender, &mut last_schedule);
    }

    log::debug!("Scheduler {} closed", name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::scheduler::protocol::SchedulerRegistration;
    use crate::scheduler::{
        drive_scheduler, prepare_scheduler_comm, FromSchedulerMessage, Scheduler, TaskAssignment,
        ToSchedulerMessage,
    };
    use std::collections::vec_deque::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
    use tokio::task::JoinHandle;
    use tokio::time::delay_for;

    #[tokio::test]
    async fn dont_schedule_without_messages() {
        let (schedule_times, tx, _rx, handle) = create_ctx(Duration::from_millis(5), vec![]);
        delay_for(Duration::from_millis(100)).await;

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert!(schedule_times.is_empty());
    }

    #[tokio::test]
    async fn dont_schedule_if_not_needed() {
        let (schedule_times, tx, _rx, handle) = create_ctx(Duration::from_millis(5), vec![false]);
        tx.send(vec![]).unwrap();
        delay_for(Duration::from_millis(100)).await;

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert!(schedule_times.is_empty());
    }

    #[tokio::test]
    async fn schedule_immediately_after_first_message() {
        let (schedule_times, tx, _rx, handle) = create_ctx(Duration::from_millis(1000), vec![true]);
        tx.send(vec![]).unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert_eq!(schedule_times.len(), 1);
    }

    #[tokio::test]
    async fn schedule_immediately_after_long_delay() {
        let msd = Duration::from_millis(500);
        let (schedule_times, tx, _rx, handle) = create_ctx(msd, vec![true; 3]);
        tx.send(vec![]).unwrap(); // schedule immediately
        tx.send(vec![]).unwrap(); // batch

        delay_for(msd * 2).await;

        tx.send(vec![]).unwrap(); // schedule immediately

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert_eq!(schedule_times.len(), 3);
    }

    #[tokio::test]
    async fn batch_schedules() {
        let msd = Duration::from_millis(100);
        let (schedule_times, tx, _rx, handle) = create_ctx(msd, vec![true; 10]);
        for _ in 0..10 {
            tx.send(vec![]).unwrap();
        }

        delay_for(msd * 2).await;

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert_eq!(schedule_times.len(), 2);
        assert!(schedule_times[1] - schedule_times[0] >= msd);
    }

    #[tokio::test]
    async fn zero_msd() {
        let msd = Duration::from_millis(0);
        let (schedule_times, tx, _rx, handle) = create_ctx(msd, vec![true; 10]);
        for _ in 0..10 {
            tx.send(vec![]).unwrap();
        }

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert_eq!(schedule_times.len(), 10);
    }

    #[tokio::test]
    async fn dont_cancel_batch() {
        let msd = Duration::from_millis(0);
        let (schedule_times, tx, _rx, handle) = create_ctx(msd, vec![true, true, false]);
        for _ in 0..3 {
            tx.send(vec![]).unwrap();
        }

        delay_for(msd * 2).await;

        drop(tx);
        handle.await.unwrap().unwrap();

        let schedule_times = schedule_times.lock().unwrap();
        assert_eq!(schedule_times.len(), 2);
    }

    fn create_ctx(
        msd: Duration,
        responses: Vec<bool>,
    ) -> (
        Arc<Mutex<Vec<Duration>>>,
        UnboundedSender<Vec<ToSchedulerMessage>>,
        UnboundedReceiver<FromSchedulerMessage>,
        JoinHandle<crate::Result<()>>,
    ) {
        let start = Instant::now();
        let schedule_times: Arc<Mutex<Vec<Duration>>> = Default::default();
        let scheduler = TestScheduler::new(responses, schedule_times.clone(), start);
        let (comm, tx, rx) = prepare_scheduler_comm();
        let handle = tokio::spawn(drive_scheduler(scheduler, comm, msd));
        (schedule_times, tx, rx, handle)
    }

    struct TestScheduler {
        responses: VecDeque<bool>,
        schedule_times: Arc<Mutex<Vec<Duration>>>,
        start: Instant,
    }
    impl TestScheduler {
        fn new(
            responses: Vec<bool>,
            schedule_times: Arc<Mutex<Vec<Duration>>>,
            start: Instant,
        ) -> Self {
            Self {
                responses: responses.into(),
                schedule_times,
                start,
            }
        }
    }
    impl Scheduler for TestScheduler {
        fn identify(&self) -> SchedulerRegistration {
            SchedulerRegistration {
                protocol_version: 0,
                scheduler_name: "".to_string(),
                scheduler_version: "".to_string(),
            }
        }

        fn handle_messages(&mut self, _messages: Vec<ToSchedulerMessage>) -> bool {
            self.responses.pop_front().unwrap()
        }

        fn schedule(&mut self) -> Vec<TaskAssignment> {
            self.schedule_times
                .lock()
                .unwrap()
                .push(self.start.elapsed());
            Default::default()
        }
    }

    impl Drop for TestScheduler {
        fn drop(&mut self) {
            assert!(self.responses.is_empty());
        }
    }
}
