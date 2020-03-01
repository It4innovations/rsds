import json

import tqdm


def chrome_method_event(timestamp, process, method, start):
    return {
        "name": method,
        "ts": timestamp,
        "ph": "B" if start else "E",
        "pid": process,
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

            if action == "measure":
                events.append(
                    chrome_method_event(timestamp, fields["process"], fields["method"], fields["event"] == "start"))
            elif action == "compute-task":
                is_start = fields["event"] == "start"
                worker_id = fields["worker"]
                task_id = fields["task"]
                events.append(chrome_worker_compute_event(timestamp, worker_id, task_id, is_start))

    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)
