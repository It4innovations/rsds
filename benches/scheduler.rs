use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rsds::scheduler::protocol::{TaskInfo, TaskUpdate, TaskUpdateType, WorkerInfo};
use rsds::scheduler::ToSchedulerMessage;
use rsds::scheduler::{Scheduler, WorkstealingScheduler};

fn create_scheduler(workers: u64) -> WorkstealingScheduler {
    let mut scheduler = WorkstealingScheduler::default();
    let workers = create_workers(workers);
    scheduler.handle_messages(workers);
    scheduler
}

fn new_tasks(count: u64) -> Vec<ToSchedulerMessage> {
    (0..count)
        .map(|i| {
            ToSchedulerMessage::NewTask(TaskInfo {
                id: i as u64,
                inputs: vec![],
            })
        })
        .collect()
}

fn create_workers(count: u64) -> Vec<ToSchedulerMessage> {
    (0..count)
        .map(|i| {
            ToSchedulerMessage::NewWorker(WorkerInfo {
                id: i as u64,
                n_cpus: 1,
                hostname: format!("worker-{}", i),
            })
        })
        .collect()
}

fn remove_tasks(count: u64) -> Vec<ToSchedulerMessage> {
    (0..count)
        .map(|i| ToSchedulerMessage::RemoveTask(i as u64))
        .collect()
}

fn finish_tasks(count: u64, worker_count: u64) -> Vec<ToSchedulerMessage> {
    (0..count)
        .map(|i| {
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: i,
                state: TaskUpdateType::Finished,
                worker: i % worker_count,
                size: Some(i),
            })
        })
        .collect()
}

fn run_schedule<S: Scheduler>(scheduler: &mut S) {
    scheduler.schedule();
}

pub fn update(c: &mut Criterion) {
    for i in &[1000, 5000, 10000] {
        c.bench_with_input(BenchmarkId::new("New tasks", i), i, |b, &i| {
            b.iter_with_setup(
                || (create_scheduler(4), new_tasks(i)),
                |(mut sched, tasks)| sched.handle_messages(tasks),
            )
        });
        c.bench_with_input(BenchmarkId::new("Schedule", i), i, |b, &i| {
            b.iter_with_setup(
                || {
                    let mut scheduler = create_scheduler(4);
                    scheduler.handle_messages(new_tasks(i));
                    scheduler
                },
                |mut sched| run_schedule(&mut sched),
            )
        });
        c.bench_with_input(BenchmarkId::new("Finish tasks", i), i, |b, &i| {
            b.iter_with_setup(
                || {
                    let mut scheduler = create_scheduler(4);
                    scheduler.handle_messages(new_tasks(i));
                    run_schedule(&mut scheduler);
                    (scheduler, finish_tasks(i, 4))
                },
                |(mut sched, tasks)| sched.handle_messages(tasks),
            )
        });
        c.bench_with_input(BenchmarkId::new("Remove tasks", i), i, |b, &i| {
            b.iter_with_setup(
                || {
                    let mut scheduler = create_scheduler(4);
                    scheduler.handle_messages(new_tasks(i));
                    run_schedule(&mut scheduler);
                    scheduler.handle_messages(finish_tasks(i, 4));
                    let tasks = remove_tasks(i);
                    (scheduler, tasks)
                },
                |(mut sched, tasks)| sched.handle_messages(tasks),
            )
        });
    }
}

criterion_group!(scheduler, update);
criterion_main!(scheduler);
