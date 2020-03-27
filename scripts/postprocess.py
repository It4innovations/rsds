import os
from multiprocessing import Pool

import click
from monitor.report import generate, serve
from monitor.src.cluster import CLUSTER_FILENAME
from postprocessing.charts import generate_charts
from postprocessing.summary import generate_summary
from postprocessing.trace import generate_chrome_trace, generate_trace_summary, generate_timeline, generate_graph, generate_trace_charts


@click.command()
@click.argument("trace-path")
@click.argument("output")
@click.option("--pretty/--no-pretty", default=False)
def trace_chrome(trace_path, output, pretty):
    generate_chrome_trace(trace_path, output, pretty)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def trace_summary(trace_path, output):
    generate_trace_summary(trace_path, output)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def trace_charts(trace_path, output):
    generate_trace_charts(trace_path, output)

@click.command()
@click.argument("trace-path")
@click.argument("output")
@click.option("--task-filter", default="")
def trace_timeline(trace_path, output, task_filter):
    generate_timeline(trace_path, output, task_filter)


@click.command()
@click.argument("trace-path")
@click.argument("output")
def trace_graph(trace_path, output):
    generate_graph(trace_path, output)


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
        fns = [
            ("trace-chrome.json", lambda file: generate_chrome_trace(trace, file, False)),
            ("trace-summary", lambda file: generate_trace_summary(trace, file)),
            ("trace-timeline.html", lambda file: generate_timeline(trace, file))
        ]

        try:
            import pygraphviz
            fns.append(("trace-graph", lambda file: generate_graph(trace, file)))
        except:
            print("Warning: pygraphviz not present, skipping graphs")

        for (file, fn) in fns:
            output = os.path.join(path, file)
            print(f"Generating {output}")
            fn(output)


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
    cli.add_command(trace_summary)
    cli.add_command(trace_charts)
    cli.add_command(trace_chrome)
    cli.add_command(trace_timeline)
    cli.add_command(trace_graph)
    cli.add_command(monitor_html)
    cli.add_command(monitor_serve)
    cli.add_command(summary)
    cli.add_command(charts)
    cli.add_command(all)
    cli()
