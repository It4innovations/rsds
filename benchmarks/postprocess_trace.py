import json

import click
import tqdm


def get_process_id(fields):
    process = fields["process"]
    if process == "scheduler":
        return process
    return f"worker-{process}"


def get_action(fields):
    action = fields["action"]
    if action == "compute-task":
        return f"compute-{fields['task']}"
    return action


@click.command()
@click.argument("trace-path")
@click.argument("output")
@click.option("--pretty/--no-pretty", default=False)
def generate_trace_viewer(trace_path, output, pretty):
    events = []
    worker_tasks = {}

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)
            timestamp = int(record["timestamp"])
            fields = record["fields"]
            process = get_process_id(fields)
            if process != "scheduler" and process not in worker_tasks:
                worker_tasks[process] = set()

            if fields["action"] == "compute-task":
                worker_tasks[process].add(fields["task"])

            action = get_action(fields)
            event_type = fields["event"]

            event = {
                "name": action,
                "ph": "B" if event_type == "start" else "E",
                "ts": timestamp,
                "pid": process,
            }

            if fields["action"] == "compute-task":
                event["id"] = action
                event["cat"] = "compute-task"
                event["ph"] = "b" if event_type == "start" else "e"

            events.append(event)
    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)

    for (worker, tasks) in sorted(worker_tasks.items()):
        print(f"{worker}: {len(tasks)} tasks")


if __name__ == "__main__":
    generate_trace_viewer()
