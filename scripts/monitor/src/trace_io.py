import datetime
import glob
import json
import os
import re
import subprocess
import time
import signal

import pandas as pd


class TraceReport:
    @staticmethod
    def load(cluster):
        workdir = cluster.workdir

        def load_data(key):
            nodes = {}
            for (node, file) in iter_files(workdir, key):
                frame = load_json_line_terminated(file)
                nodes[node] = frame_create_datetime(frame)
            return nodes

        monitor = load_data("monitor")

        return TraceReport(cluster, monitor, load_outputs(workdir, 100))

    def __init__(self, cluster, monitor, outputs):
        self.cluster = cluster
        self.monitor = monitor
        self.outputs = outputs


def iter_files(dir, prefix):
    for file in glob.glob(f"{dir}/{prefix}-*.trace"):
        basename = os.path.basename(file)
        node = re.match(rf"^{prefix}-(.*)\.trace$", basename).group(1)
        yield (node, os.path.abspath(file))


def frame_create_datetime(frame):
    if len(frame) > 0:
        frame["datetime"] = frame["timestamp"].apply(lambda t: datetime.datetime.utcfromtimestamp(t))
        frame["datetime"] = pd.to_datetime(frame["datetime"])
    else:
        frame["datetime"] = []
    return frame


def load_json_line_terminated(path):
    records = []

    with open(path) as file:
        for line in file.readlines():
            batch = json.loads(line)
            records.append(batch)

    return pd.DataFrame.from_records(records)


def load_outputs(workdir, max=None):
    outputs = {}

    def gather_outputs(suffix):
        for file in glob.glob(os.path.join(workdir, f"*.{suffix}")):
            node = os.path.splitext(os.path.basename(file))[0]
            if node not in outputs:
                outputs[node] = {}

            args = ["cat", file]
            if max is not None:
                args = ["tail", f"-{max}", file]
            outputs[node][suffix] = subprocess.run(args, stdout=subprocess.PIPE,
                                                   stderr=subprocess.PIPE,
                                                   stdin=subprocess.PIPE).stdout.decode()

    gather_outputs("out")
    gather_outputs("err")

    return outputs


def dump_records(file, records):
    if records:
        with open(file, "a") as f:
            for record in records:
                json.dump(record, f)
                f.write("\n")
        records.clear()


def trace_process(capture_interval, dump_interval, file, capture_fn, finish_fn):
    record_count = 0

    records = []
    last_update = time.time()

    def finish(sig, frame):
        nonlocal record_count

        record_count += len(records)
        dump_records(file, records)
        print(f"Interrupting monitoring, wrote {record_count} records")
        finish_fn()

    signal.signal(signal.SIGINT, finish)

    while True:
        time.sleep(capture_interval)

        now = time.time()
        record = capture_fn(now)
        if record is not None:
            records.append(record)

        if now - last_update > dump_interval:
            last_update = now
            record_count += len(records)
            dump_records(file, records)
