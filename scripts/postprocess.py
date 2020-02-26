import os
from trace import generate_chrome_trace, generate_trace_summary
from multiprocessing import Pool

import click
from monitor.report import generate, serve
from monitor.src.cluster import CLUSTER_FILENAME


@click.command()
@click.argument("trace-path")
@click.argument("output")
@click.option("--pretty/--no-pretty", default=False)
def chrome_trace(trace_path, output, pretty):
    generate_chrome_trace(trace_path, output, pretty)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def summary(trace_path, output):
    generate_trace_summary(trace_path, output)


@click.command()
@click.argument("cluster-file")
@click.option("--output", default="output.html")
def monitor_html(cluster_file, output):
    generate(cluster_file, output)


@click.command()
@click.argument("cluster-file")
@click.option("--port", default=5556)
def monitor_serve(cluster_file, port):
    serve(cluster_file, port)


def generate_dir(path):
    cluster = os.path.join(path, CLUSTER_FILENAME)
    if os.path.isfile(cluster):
        monitor_output = os.path.join(path, "monitor.html")
        print(f"Generating monitor HTML: {monitor_output}")
        generate(cluster, monitor_output)
    trace = os.path.join(path, "scheduler.trace")
    if os.path.isfile(trace):
        chrome_trace = os.path.join(path, "chrome.json")
        print(f"Generating Chrome trace: {chrome_trace}")
        generate_chrome_trace(trace, chrome_trace, False)

        trace_summary = os.path.join(path, "trace-summary.txt")
        print(f"Generating trace summary: {trace_summary}")
        generate_trace_summary(trace, trace_summary)


@click.command()
@click.argument("directory")
def all(directory):
    dirs = [os.path.join(directory, subdir) for subdir in os.listdir(directory)]
    with Pool() as pool:
        pool.map(generate_dir, dirs)


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(chrome_trace)
    cli.add_command(summary)
    cli.add_command(monitor_html)
    cli.add_command(monitor_serve)
    cli.add_command(all)
    cli()
