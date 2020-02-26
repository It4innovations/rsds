import os
import shutil
import sys
import time

import click
import psutil
from src.trace_io import trace_process


def get_resources():
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent
    connections = sum(1 if c[5] == "ESTABLISHED" else 0 for c in psutil.net_connections())
    bytes = psutil.net_io_counters()
    io = None # psutil.disk_io_counters()

    return {
        "cpu": cpus,
        "mem": mem,
        "connections": connections,
        "net-write": 0 if bytes is None else bytes.bytes_sent,
        "net-read": 0 if bytes is None else bytes.bytes_recv,
        "disk-write": 0 if io is None else io.write_bytes,
        "disk-read": 0 if io is None else io.read_bytes
    }


def generate_record(timestamp):
    resources = get_resources()

    return {
        "timestamp": timestamp,
        "resources": resources,
    }


@click.command()
@click.argument("output")
@click.option("--capture-interval", default=1)
@click.option("--dump-interval", default=10)
def main(output, capture_interval, dump_interval):
    def capture(timestamp):
        try:
            return generate_record(timestamp)
        except Exception as e:
            print("Opening cluster exception: {}".format(e))
            return None

    def finish():
        print(f"Copying trace from {tmp_output} to {output}")
        shutil.copyfile(tmp_output, output)
        sys.exit()

    tmp_output = f"/tmp/{os.path.basename(output)}-{int(time.time())}"

    # create tmp file
    with open(tmp_output, "w") as f:
        pass

    trace_process(capture_interval, dump_interval, tmp_output, capture, finish)


if __name__ == "__main__":
    main()
