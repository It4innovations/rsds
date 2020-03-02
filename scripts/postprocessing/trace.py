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

    def finish_task(self, task_id, timestamp, duration):
        assert task_id in self.started_tasks
        self.tasks[task_id] = (task_id, timestamp - self.started_tasks[task_id], duration)
        del self.started_tasks[task_id]


def generate_trace_summary(trace_path, output):
    workers = {}
    measurements = {}
    task_durations = {}
    action_timestamps = {}
    steals = []
    packets_sent = []
    packets_received = []

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)
            timestamp = int(record["timestamp"])
            fields = record["fields"]
            action = fields["action"]

            if action == "measure":
                method = fields["method"]
                is_start = fields["event"] == "start"
                if is_start:
                    assert method not in action_timestamps
                    action_timestamps[method] = timestamp
                else:
                    assert method in action_timestamps
                    measurements.setdefault(method, []).append(timestamp - action_timestamps[method])
                    del action_timestamps[method]
            elif action == "compute-task":
                is_start = fields["event"] == "start"
                worker_id = fields["worker"]
                task_id = fields["task"]

                assert worker_id in workers
                if not is_start:
                    duration = fields["duration"]
                    task_durations[task_id] = duration
                    workers[worker_id].finish_task(task_id, timestamp, duration)
                else:
                    workers[worker_id].start_task(task_id, timestamp)
            elif action == "new-worker":
                id = fields["worker_id"]
                assert id not in workers
                workers[id] = Worker(id, fields["cpus"])
            elif action == "steal":
                steals.append("steal")
            elif action == "steal-response":
                result = fields["result"]
                steals.append(result)
            elif action == "packet-send":
                packets_sent.append(fields["size"])
            elif action == "packet-receive":
                packets_received.append(fields["size"])
            else:
                raise Exception(f"Unknown action {action}")

        with open(output, "w") as f:
            f.write("---WORKER summary---\n")
            for worker in sorted(workers.values(), key=lambda w: len(w.tasks), reverse=True):
                worker_task_durations = pd.Series([t[1] for t in worker.tasks.values()]) / 1000
                worker_compute_durations = pd.Series([t[2] for t in worker.tasks.values()]) / 1000
                f.write(
                    f"Worker {worker.id}: {len(worker.tasks)} tasks, "
                    f"mean task duration: {worker_task_durations.mean()}, "
                    f"mean compute duration: {worker_compute_durations.mean()}\n")
            f.write("\n")

            with pd.option_context("display.float_format", lambda x: f"{x:.3f}"):
                f.write("---MEASURE summary---\n")
                for (method, times) in measurements.items():
                    f.write(f"---{method}---\n")
                    f.write(f"{(pd.Series(times) / 1000).describe()}\n\n")

                f.write("---STEAL summary---\n")
                f.write(f"{pd.Series(steals).value_counts()}\n\n")

                f.write("---TASK summary---\n")
                tasks = pd.Series(list(task_durations.values())) / 1000
                f.write(f"{tasks.describe()}\n\n")

            with pd.option_context("display.float_format", lambda x: f"{x:.0f}"):
                to_mb = 1024 * 1024
                f.write("---PACKET summary---\n")
                sent = pd.Series(packets_sent)
                f.write(f"Sent:\n{sent.describe()}\nSum: {sent.sum() / to_mb} MiB\n\n")
                received = pd.Series(packets_received)
                f.write(f"Received:\n{received.describe()}\nSum: {received.sum() / to_mb} MiB\n\n")
