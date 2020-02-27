import json

import tqdm


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

    with open(trace_path) as f:
        for line in tqdm.tqdm(f):
            record = json.loads(line)
            timestamp = int(record["timestamp"])
            fields = record["fields"]
            action = fields["action"]

            if action == "schedule":
                events.append(chrome_schedule_event(timestamp, fields["event"] == "start"))
            elif action == "compute-task":
                is_start = fields["event"] == "start"
                worker_id = fields["worker"]
                task_id = fields["task"]
                events.append(chrome_worker_compute_event(timestamp, worker_id, task_id, is_start))

    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)
