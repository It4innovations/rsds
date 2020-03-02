import os
from multiprocessing import Pool

import click
from monitor.report import generate, serve
from monitor.src.cluster import CLUSTER_FILENAME
from postprocessing.charts import generate_charts
from postprocessing.chrome import generate_chrome_trace
from postprocessing.summary import generate_summary
from postprocessing.timeline import generate_timeline
from postprocessing.trace import generate_trace_summary


@click.command()
@click.argument("trace-path")
@click.argument("output")
@click.option("--pretty/--no-pretty", default=False)
def chrome_trace(trace_path, output, pretty):
    generate_chrome_trace(trace_path, output, pretty)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def trace_summary(trace_path, output):
    generate_trace_summary(trace_path, output)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def timeline(trace_path, output):
    generate_timeline(trace_path, output)


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


@click.command()
@click.argument("result")
@click.option("--output-dir", default=None)
def summary(result, output_dir):
    generate_summary(result, output_dir)


@click.command()
@click.argument("result")
@click.option("--output-dir", default=None)
def charts(result, output_dir):
    generate_charts(result, output_dir)


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

        trace_timeline = os.path.join(path, "trace-timeline.html")
        print(f"Generating timeline: {trace_timeline}")
        generate_timeline(trace, trace_timeline)


@click.command()
@click.argument("directory")
def all(directory):
    result_frame = os.path.join(directory, "result.json")
    if os.path.isfile(result_frame):
        generate_summary(result_frame, directory)
        generate_charts(result_frame, directory)

    dirs = [os.path.join(directory, subdir) for subdir in os.listdir(directory)]
    with Pool() as pool:
        pool.map(generate_dir, dirs)


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(chrome_trace)
    cli.add_command(trace_summary)
    cli.add_command(timeline)
    cli.add_command(monitor_html)
    cli.add_command(monitor_serve)
    cli.add_command(summary)
    cli.add_command(charts)
    cli.add_command(all)
    cli()
