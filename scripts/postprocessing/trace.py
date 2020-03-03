import json
from typing import Tuple, Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import tqdm

from .bokeh_timeline import TaskStartTraceEvent, TaskEndTraceEvent, normalize_task_events, \
    plot_tabs, build_plot, plot_task_lifespan, plot_tasks_on_workers, format_bytes


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
        wait_duration = timestamp - self.started_tasks[task_id]
        self.tasks[task_id] = (task_id, wait_duration, duration)
        del self.started_tasks[task_id]
        return wait_duration


class Task:
    def __init__(self, name, key, inputs, timestamp):
        self.name = name
        self.id = name
        self.duration = None  # compute duration
        self.wait_duration = None  # assign -> finish in scheduler
        self.size = None
        self.worker = None
        self.inputs = inputs
        self.cpus = 1
        self.key = key
        self.events = []
        self.add_event("create", timestamp)

    def add_event(self, event, timestamp, args=None):
        if args is None:
            args = {}
        self.events.append((event, timestamp, args))

    def input_bytes(self):
        return sum(t.size for t in self.inputs)


def generate_trace_summary(trace_path, output):
    measurements = {}
    action_timestamps = {}
    steals = []
    packets_sent = []
    packets_received = []

    def normalize_time(_, time):
        return time / 1000

    def handle_event(timestamp, fields, tasks, workers):
        action = fields["action"]

        if action == "measure":
            process = fields['process']
            if ":" in process:
                process = process[:process.find(":")]
            method = f"{process}/{fields['method']}"
            is_start = fields["event"] == "start"
            if is_start:
                assert method not in action_timestamps
                action_timestamps[method] = timestamp
            else:
                assert method in action_timestamps
                measurements.setdefault(method, []).append(timestamp - action_timestamps[method])
                del action_timestamps[method]
        elif action == "steal":
            steals.append("steal")
        elif action == "steal-response":
            steals.append(fields["result"])
        elif action == "packet-send":
            packets_sent.append(fields["size"])
        elif action == "packet-receive":
            packets_received.append(fields["size"])

    tasks, workers = parse_trace(trace_path, handle_event, normalize_time=normalize_time)

    plt.clf()
    compute_durations = [t.duration for t in tasks.values()]
    ax = sns.distplot(compute_durations)
    ax.set(xlabel='Compute duration [ms]')
    plt.savefig(f"{output}-compute.png")

    plt.clf()
    wait_durations = [t.wait_duration for t in tasks.values()]
    ax = sns.distplot(wait_durations)
    ax.set(xlabel='Wait duration [ms]')
    plt.savefig(f"{output}-wait.png")

    with open(f"{output}.txt", "w") as f:
        with pd.option_context("display.float_format", lambda x: f"{x:.3f}"):
            f.write("---TASK summary---\n")
            f.write("Longest tasks:\n")
            for task in sorted(tasks.values(), key=lambda t: t.wait_duration, reverse=True)[:20]:
                f.write(
                    f"Task {task.id}: wait {task.wait_duration:.4} ms, compute {task.duration:.4} ms, "
                    f"worker {task.worker.id}, "
                    f"size: {format_bytes(task.size)}, inputs: {len(task.inputs)}, "
                    f"input_bytes: {format_bytes(task.input_bytes())}\n")
            f.write("...\n\n")
            f.write("Task compute summary:\n")
            f.write(f"{pd.Series([t.duration for t in tasks.values()]).describe()}\n\n")
            f.write("Task wait summary:\n")
            f.write(f"{pd.Series([t.wait_duration for t in tasks.values()]).describe()}\n\n")

            f.write("---WORKER summary---\n")
            for worker in sorted(workers.values(), key=lambda w: len(w.tasks), reverse=True):
                worker_task_durations = pd.Series([t[1] for t in worker.tasks.values()])
                worker_compute_durations = pd.Series([t[2] for t in worker.tasks.values()])
                f.write(
                    f"Worker {worker.id}: {len(worker.tasks)} tasks, "
                    f"mean task duration: {worker_task_durations.mean()}, "
                    f"mean compute duration: {worker_compute_durations.mean()}\n")
                f.write("\n")

            f.write("---MEASURE summary---\n")
            for (method, times) in measurements.items():
                f.write(f"---{method}---\n")
                f.write(f"{pd.Series(times).describe()}\n\n")

            f.write("---STEAL summary---\n")
            f.write(f"{pd.Series(steals).value_counts()}\n\n")

        with pd.option_context("display.float_format", lambda x: f"{x:.0f}"):
            f.write("---PACKET summary---\n")
            sent = pd.Series(packets_sent)
            f.write(f"Sent:\n{sent.describe()}\nSum: {format_bytes(sent.sum())}\n\n")
            received = pd.Series(packets_received)
            f.write(f"Received:\n{received.describe()}\nSum: {format_bytes(received.sum())}\n\n")


def generate_timeline(trace_path, output, task_filter=None):
    events = []
    first_timestamp = None
    max_normalized_time = 0

    def normalize_time(fields, timestamp):
        nonlocal first_timestamp, max_normalized_time

        timestamp = time_us_to_s(timestamp)

        if fields["action"] == "task" and first_timestamp is None:
            first_timestamp = timestamp

        if first_timestamp is not None:
            timestamp -= first_timestamp
            max_normalized_time = max(max_normalized_time, timestamp)
        assert timestamp >= 0
        return timestamp

    def handle_event(timestamp, fields, tasks, workers):
        if fields["action"] == "task" and fields["event"] == "finish":
            start = normalize_time(fields, fields["start"])
            end = normalize_time(fields, fields["stop"])

            task = tasks[fields["task"]]
            worker = workers[fields["worker"]]
            events.append(TaskStartTraceEvent(time=start, worker=worker, task=task))
            events.append(TaskEndTraceEvent(time=end, worker=worker, task=task))

    tasks, workers = parse_trace(trace_path, handle_event, normalize_time=normalize_time)

    events = normalize_task_events(events)
    worker_list = sorted(workers.values(), key=lambda w: w.id)

    def plot_fn():
        return plot_tabs([
            (lambda: plot_task_lifespan(tasks, max_normalized_time, task_filter), "Task lifespan"),
            (lambda: plot_tasks_on_workers(events, worker_list), "Tasks on workers"),
        ])

    build_plot(plot_fn, output)


def generate_graph(trace_path, output):
    import pygraphviz as pgv

    graph = pgv.AGraph(directed=True)

    def handle_event(timestamp, fields, tasks, workers):
        pass

    tasks, workers = parse_trace(trace_path, handle_event)

    max_duration = max([t.duration for t in tasks.values()], default=0)
    max_wait_duration = max([t.wait_duration for t in tasks.values()], default=0)
    max_size = max([t.size for t in tasks.values()], default=0)
    max_line_width = 20
    max_node_dimension = 4

    for task in tasks.values():
        worker = task.worker
        color = f"{0.3 - (task.wait_duration / max_wait_duration) * 0.3} 1 1"
        dimension = 1 + (task.duration / max_duration) * max_node_dimension
        graph.add_node(task.id,
                       label=f"{worker.id}: {task.id}",
                       tooltip=f"key: {task.key}, duration: {task.duration}, wait: {task.wait_duration}, "
                               f"inputs: {', '.join(str(i.id) for i in task.inputs)}",
                       style="filled",
                       fixedsize=True,
                       width=dimension,
                       height=dimension,
                       fillcolor=color,
                       fontname="bold")

        for input in task.inputs:
            src_worker = input.worker
            args = {"fontname": "bold"}
            if src_worker.id != worker.id:
                args["label"] = format_bytes(task.size)
                args["tooltip"] = f"{src_worker.id}->{worker.id}: {format_bytes(task.size)}"
                args["penwidth"] = 1 + (task.size / max_size) * max_line_width
            else:
                args["style"] = "dotted"

            graph.add_edge(input.id, task.id, **args)

    graph.write(f"{output}.dot")
    graph.layout("dot")
    graph.draw(f"{output}.svg")


def chrome_method_event(timestamp, process, method, start):
    return {
        "name": method,
        "ts": timestamp,
        "ph": "B" if start else "E",
        "pid": process,
    }


def chrome_worker_compute_event(timestamp, worker_id, task_id, start, name=None):
    compute_id = f"compute-{task_id}"
    return {
        "name": name if name else compute_id,
        "ts": timestamp,
        "ph": "b" if start else "e",
        "pid": f"worker-{worker_id}",
        "id": compute_id,
        "cat": "compute-task"
    }


def generate_chrome_trace(trace_path, output, pretty):
    events = []

    def normalize_time(_, time):
        return time

    def handle_event(timestamp, fields, tasks, workers):
        action = fields["action"]

        if action == "measure":
            events.append(
                chrome_method_event(timestamp, fields["process"], fields["method"], fields["event"] == "start"))
        elif action == "compute-task":
            is_start = fields["event"] == "start"
            worker_id = fields["worker"]
            task_id = fields["task"]
            events.append(chrome_worker_compute_event(timestamp, worker_id, task_id, is_start))
            if not is_start:
                events.append(
                    chrome_worker_compute_event(time_us_to_s(fields["start"]), worker_id, task_id, True,
                                                name="compute"))
                events.append(
                    chrome_worker_compute_event(time_us_to_s(fields["stop"]), worker_id, task_id, False,
                                                name="compute"))

    parse_trace(trace_path, handle_event, normalize_time=normalize_time)

    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)


def time_us_to_s(time):
    return int(time) / (1000 * 1000)


def parse_trace(trace_path, handle_event, normalize_time=None) -> Tuple[Dict[int, Task], Dict[int, Worker]]:
    workers = {}
    tasks = {}

    if normalize_time is None:
        def normalize_time(_, time):
            return time_us_to_s(time)

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)

            fields = record["fields"]
            timestamp = normalize_time(fields, int(record["timestamp"]))
            action = fields["action"]

            if action == "task":
                event = fields.get("event")
                task_id = fields["task"]
                if event == "create":
                    assert task_id not in tasks
                    inputs = tuple(int(i) for i in fields.get("inputs", ()).rstrip(",").split(",") if i)
                    tasks[task_id] = Task(task_id, fields["key"], inputs, timestamp)
                else:
                    worker_id = fields["worker"]
                    assert worker_id in workers
                    worker = workers[worker_id]
                    task = tasks[task_id]
                    task.add_event(event, timestamp, {"worker": worker})

                    if event == "assign":
                        worker.start_task(task_id, timestamp)
                    elif event == "finish":
                        start = normalize_time(fields, fields["start"])
                        end = normalize_time(fields, fields["stop"])

                        duration = end - start
                        task.duration = duration
                        task.size = fields["size"]
                        task.inputs = tuple(tasks[i] for i in task.inputs)
                        for t in task.inputs:
                            assert t.duration is not None

                        task.add_event("compute-start", start, {"worker": worker})
                        task.add_event("compute-end", end, {"worker": worker})

                        task.worker = worker
                        task.wait_duration = worker.finish_task(task_id, timestamp, duration)
            elif action == "new-worker":
                worker_id = fields["worker_id"]
                assert worker_id not in workers
                workers[worker_id] = Worker(worker_id, fields["cpus"])

            handle_event(timestamp, fields, tasks, workers)

    return (tasks, workers)
