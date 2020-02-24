import click
import psutil
from src.trace_io import trace_process


def get_resources():
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent
    connections = len(psutil.net_connections())
    bytes = psutil.net_io_counters()
    io = psutil.disk_io_counters()

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

    trace_process(capture_interval, dump_interval, output, capture)


if __name__ == "__main__":
    main()
