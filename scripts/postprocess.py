import os
from trace import generate_chrome_trace, generate_trace_summary
from multiprocessing import Pool

import click
import pandas as pd
import seaborn as sns

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
def trace_summary(trace_path, output):
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


def create_plot(frame, plot_fn):
    def extract(fn):
        items = list(fn.split("-"))
        for i in range(len(items)):
            try:
                num = float(items[i])
                items[i] = num
            except:
                pass
        return tuple(items)

    clusters = sorted(set(frame["cluster"]))
    functions = sorted(set(frame["function"]), key=extract)

    def plot(data, **kwargs):
        plot_fn(data, clusters, **kwargs)

    g = sns.FacetGrid(frame, col="function", col_wrap=4, col_order=functions, sharey=False)
    g = g.map_dataframe(plot)
    g = g.add_legend()
    g.set_ylabels("Time [ms]")
    g.set(ylim=(0, None))
    g.set_xticklabels(rotation=90)
    return g


def generate_summary(result, directory):
    directory = directory or os.getcwd()
    frame = pd.read_json(result)

    def plot_box(data, clusters, **kwargs):
        sns.boxplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    def plot_scatter(data, clusters, **kwargs):
        sns.swarmplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    if len(frame) > 0:
        for (file, plot_fn) in (
                ("result_boxplot", plot_box),
                ("result_scatterplot", plot_scatter)
        ):
            plot = create_plot(frame, plot_fn)
            plot.savefig(os.path.join(directory, f"{file}.png"))

    def describe(frame, file):
        if len(frame) > 0:
            s = frame.groupby(["function", "cluster"])["time"].describe()
            file.write(f"{s}\n")
            s = frame.groupby(["cluster"])["time"].describe()
            file.write(f"{s}\n")
        else:
            file.write("(empty dataframe)\n")

    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.expand_frame_repr', False):
        with open(os.path.join(directory, "summary.txt"), "w") as f:
            f.write("All results:\n")
            describe(frame, f)
            f.write("Results without first run:\n")
            describe(frame[frame["index"] != 0], f)


@click.command()
@click.argument("result")
@click.option("--output-dir", default=None)
def summary(result, output_dir):
    generate_summary(result, output_dir)


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
    result_frame = os.path.join(directory, "result.json")
    if os.path.isfile(result_frame):
        generate_summary(result_frame, directory)

    dirs = [os.path.join(directory, subdir) for subdir in os.listdir(directory)]
    with Pool() as pool:
        pool.map(generate_dir, dirs)


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(chrome_trace)
    cli.add_command(trace_summary)
    cli.add_command(monitor_html)
    cli.add_command(monitor_serve)
    cli.add_command(summary)
    cli.add_command(all)
    cli()
