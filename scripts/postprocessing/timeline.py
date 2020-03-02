import json

import tqdm

from .bokeh_trace import simulator_trace_to_html, TaskStartTraceEvent, TaskEndTraceEvent


class Worker:
    def __init__(self, id, cpus):
        self.id = id
        self.cpus = cpus


class Task:
    def __init__(self, name, worker):
        self.name = name
        self.duration = None
        self.cpus = 1
        self.worker = worker


def generate_timeline(trace_path, output):
    workers = {}
    tasks = {}
    events = []
    first_timestamp = None
    scale = 1000 * 1000

    def normalize_time(time):
        t = (int(time) / scale) - first_timestamp
        assert t >= 0
        return t

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)

            fields = record["fields"]
            action = fields["action"]

            if action == "compute-task":
                if first_timestamp is None:
                    first_timestamp = int(record["timestamp"]) / scale

                is_start = fields["event"] == "start"
                worker_id = fields["worker"]
                task_id = fields["task"]

                assert worker_id in workers
                worker = workers[worker_id]
                if not is_start:
                    start = normalize_time(fields["start"])
                    end = normalize_time(fields["stop"])
                    events.append(TaskStartTraceEvent(time=start, worker=worker, task=tasks[task_id]))
                    events.append(TaskEndTraceEvent(time=end, worker=worker, task=tasks[task_id]))
                    tasks[task_id].duration = end - start
                else:
                    if task_id not in tasks:
                        task = Task(task_id, worker)
                        tasks[task_id] = task
                    else:
                        task = tasks[task_id]
                        task.worker = worker
            elif action == "new-worker":
                id = fields["worker_id"]
                assert id not in workers
                workers[id] = Worker(id, fields["cpus"])

    simulator_trace_to_html(events, sorted(workers.values(), key=lambda w: w.id), output)
