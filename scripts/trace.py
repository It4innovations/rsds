import json

import pandas as pd
import tqdm


class Worker:
    def __init__(self, id, cpus):
        self.id = id
        self.cpus = cpus
        self.tasks = {}
        self.started_tasks = {}

    def start_task(self, task_id, timestamp):
        self.started_tasks[task_id] = timestamp

    def finish_task(self, task_id, timestamp):
        assert task_id in self.started_tasks
        self.tasks[task_id] = (task_id, timestamp - self.started_tasks[task_id])
        del self.started_tasks[task_id]


def chrome_schedule_event(timestamp, start):
    return {
        "name": "schedule",
        "ts": timestamp,
        "ph": "B" if start else "E",
        "pid": "scheduler",
    }


def chrome_worker_compute_event(timestamp, worker_id, task_id, start):
    compute_id = f"compute-{task_id}"
    return {
        "name": compute_id,
        "ts": timestamp,
        "ph": "b" if start else "e",
        "pid": f"worker-{worker_id}",
        "id": compute_id,
        "cat": "compute-task"
    }


def generate_chrome_trace(trace_path, output, pretty):
    events = []
    workers = {}
    schedule_durations = []
    last_schedule = None
    steals_started = 0
    steals_successful = 0
    steals_failed = 0

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)
            timestamp = int(record["timestamp"])
            fields = record["fields"]
            action = fields["action"]

            if action == "schedule":
                is_start = fields["event"] == "start"
                if is_start:
                    last_schedule = timestamp
                else:
                    assert last_schedule
                    schedule_durations.append(timestamp - last_schedule)
                events.append(chrome_schedule_event(timestamp, is_start))
            elif action == "compute-task":
                is_start = fields["event"] == "start"
                worker_id = fields["worker"]
                task_id = fields["task"]

                assert worker_id in workers
                if not is_start:
                    workers[worker_id].finish_task(task_id, timestamp)
                else:
                    workers[worker_id].start_task(task_id, timestamp)
                events.append(chrome_worker_compute_event(timestamp, worker_id, task_id, is_start))
            elif action == "new-worker":
                id = fields["worker_id"]
                assert id not in workers
                workers[id] = Worker(id, fields["cpus"])
            elif action == "steal":
                steals_started += 1
            elif action == "steal-response":
                if fields["success"]:
                    steals_successful += 1
                else:
                    steals_failed += 1
            elif action in {"balance"}:
                pass
            else:
                raise Exception(f"Unknown action {action}")

    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)

    for worker in sorted(workers.values(), key=lambda w: len(w.tasks), reverse=True):
        task_durations = pd.Series([t[1] for t in worker.tasks.values()]) / 1000
        print(f"Worker {worker.id}: {len(worker.tasks)} tasks, mean task duration: {task_durations.mean()}")

    print("Schedule statistics:")
    print((pd.Series(schedule_durations) / 1000).describe())
