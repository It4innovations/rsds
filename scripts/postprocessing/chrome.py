import json

import tqdm


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
                if not is_start:
                    events.append(
                        chrome_worker_compute_event(fields["start"], worker_id, task_id, True, name="compute"))
                    events.append(chrome_worker_compute_event(fields["stop"], worker_id, task_id, False, name="compute"))

    with open(output, "w") as f:
        json.dump(events, f, indent=4 if pretty else None)
