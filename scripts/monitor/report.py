import base64
import datetime
import os

import pandas as pd
from bokeh.embed import file_html
from bokeh.io import save
from bokeh.layouts import gridplot
from bokeh.models import Div, NumeralTickFormatter, Panel, Tabs, Title
from bokeh.models import Range1d
from bokeh.palettes import d3
from bokeh.plotting import figure
from bokeh.resources import CDN
from tornado import ioloop, web

from .src.cluster import Cluster
from .src.trace_io import TraceReport


def average(data):
    return sum(data) / len(data)


def resample(data, time, period="1S"):
    data.index = time
    data = data.resample(period).first()
    return data.interpolate()


def get_sorted_nodes(cluster, keys):
    nodes = []

    for key in keys:
        key_nodes = cluster.get_processes_by_key(key)
        for (node, _) in key_nodes:
            if node not in nodes:
                nodes.append(node)
    return nodes


def process_name(process):
    return process.get("name", process.get("key")) or "Unknown process"


def plot_time_chart(data, draw_fn, min_datetime, max_datetime, generate_rows=None):
    if not generate_rows:
        def generate_rows(node, frame):
            return [[(node, frame)]]

    result = []

    for (node, frame) in data:
        rows = generate_rows(node, frame)
        for row in rows:
            columns = []
            for col in row:
                f = figure(plot_width=1000, plot_height=250,
                           x_range=[min_datetime, max_datetime],
                           x_axis_type='datetime')
                draw_fn(col, f)
                columns.append(f)

            result.append(columns)
    return gridplot(result)


def plot_resources_usage(report):
    monitor = report.monitor
    nodes = get_sorted_nodes(report.cluster, ["monitor"])
    data = [(node, monitor[node]) for node in nodes if node in monitor]

    if not data:
        return Div(text="(no data)")

    datetimes = pd.concat(frame["datetime"] for (_, frame) in data)

    min_datetime = datetimes.min() - datetime.timedelta(seconds=1)
    max_datetime = datetimes.max()

    def generate_rows(node, frame):
        return [
            [
                (node, frame, "cpu"),
                (node, frame, "network")
            ],
            [
                (node, frame, "mem"),
                (node, frame, "net-connections")
            ]
        ]

    def draw(args, figure):
        node, frame, method = args
        time = frame["datetime"]
        resources = frame["resources"]

        if method == "cpu":
            figure.plot_width = 950
            figure.plot_height = 400
        else:
            figure.plot_width = 950
            figure.plot_height = 400

        figure.min_border_right = 20

        if method in ("cpu", "mem"):
            figure.y_range = Range1d(0, 1)

        if len(resources) == 0:
            return

        def draw_bytes(title, read_col, write_col):
            if resources.iloc[0].get(read_col) is None or resources.iloc[0].get(write_col) is None:
                return

            def accumulate(column):
                values = resources.apply(lambda res: res[column])
                values = values - values.min()
                return resample(values, time)

            read = accumulate(read_col)
            write = accumulate(write_col)

            figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
            figure.line(read.index, read, color="blue", legend_label="{} RX".format(title))
            figure.line(write.index, write, color="red", legend_label="{} TX".format(title))

        if method == "cpu":
            cpu_count = len(resources.iloc[0]["cpu"])
            cpus = [resample(resources.apply(lambda res: res["cpu"][i]), time) for i in range(cpu_count)]
            cpu_mean = resample(resources.apply(lambda res: average(res["cpu"])), time)

            processes = (process_name(p) for p in report.cluster.nodes[node] if process_name(p))
            figure.title = Title(text="{}: {}".format(node, ",".join(processes)))

            figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")

            palette = d3["Category20"][20]
            for (i, cpu) in enumerate(cpus):
                color = palette[i % 20]
                figure.line(cpu.index, cpu / 100.0, color=color, legend_label=f"CPU #{i}")
            figure.line(cpu_mean.index, cpu_mean / 100.0, color="red", legend_label=f"Average CPU", line_dash="dashed",
                        line_width=5)
        elif method == "mem":
            mem = resample(resources.apply(lambda res: res["mem"]), time)
            figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
            figure.line(mem.index, mem / 100.0, color="red", legend_label="Memory")
        elif method == "network":
            draw_bytes("Net", "net-read", "net-write")
        elif method == "net-connections":
            connections = resample(resources.apply(lambda res: res["connections"]), time)
            figure.line(connections.index, connections, legend_label="Network connections")
        elif method == "io":
            draw_bytes("Disk", "disk-read", "disk-write")

    return plot_time_chart(data, draw, min_datetime=min_datetime, max_datetime=max_datetime,
                           generate_rows=generate_rows)


def plot_profile(flamegraph):
    with open(flamegraph, "rb") as f:
        data = f.read()
        base64_content = base64.b64encode(data).decode()
        content = f"""<object type="image/svg+xml" width="1600px" data="data:image/svg+xml;base64,{base64_content}"></object>"""
        return Div(text=content)


def plot_output(report):
    nodes = [k for k in report.outputs.keys() if not k.startswith("monitor")]
    tabs = []

    for node in nodes:
        output_tabs = []
        for output in ("out", "err"):
            content = report.outputs[node].get(output, "")
            if content:
                content = content.replace("\n", "<br />")
                output_tabs.append(Panel(child=Div(text=content, sizing_mode='stretch_both'), title=output))

        if output_tabs:
            tabs.append(Panel(child=Tabs(tabs=output_tabs, sizing_mode='stretch_both'), title=node))

    return Tabs(tabs=tabs, sizing_mode='stretch_both')


def create_page(report, directory):
    structure = [
        ("Resources", plot_resources_usage),
        ("Output", plot_output)
    ]

    flamegraph = os.path.join(directory, "scheduler.svg")
    if os.path.isfile(flamegraph):
        structure.append(("Scheduler profile", lambda _: plot_profile(flamegraph)))

    tabs = []
    for name, fn in structure:
        f = fn(report)
        tabs.append(Panel(child=f, title=name))

    return Tabs(tabs=tabs)


def load_report(cluster_file):
    with open(cluster_file) as file:
        cluster = Cluster.deserialize(file)
        if not os.path.isdir(cluster.workdir):
            new_workdir = os.path.abspath(os.path.dirname(cluster_file))
            print(f"Cluster workdir {cluster.workdir} not found, setting to {new_workdir}")
            cluster.workdir = new_workdir
    return TraceReport.load(cluster)


def generate(cluster_file, output):
    report = load_report(cluster_file)
    directory = os.path.dirname(cluster_file)
    page = create_page(report, directory)
    save(page, output, title="Cluster monitor", resources=CDN)


def serve(cluster_file, port):
    directory = os.path.dirname(cluster_file)

    class Handler(web.RequestHandler):
        def get(self):
            report = load_report(cluster_file)
            page = create_page(report, directory)
            self.write(file_html(page, CDN, "Cluster report"))

    app = web.Application([
        (r"/", Handler),
    ])
    app.listen(port)

    print(f"Serving report at http://0.0.0.0:{port}")
    ioloop.IOLoop.current().start()
