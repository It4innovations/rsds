import os

from bokeh.embed import file_html
from bokeh.io import save
from bokeh.layouts import gridplot
from bokeh.models import Div, NumeralTickFormatter, Panel, Tabs, Title
from bokeh.plotting import figure
from bokeh.resources import CDN
from tornado import ioloop, web
import pandas as pd

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


def plot_time_chart(data, draw_fn, min_datetime, max_datetime, generate_row=None):
    if not generate_row:
        def generate_row(node, frame):
            return [(node, frame)]

    result = []

    for (node, frame) in data:
        row = generate_row(node, frame)
        columns = []
        for r in row:
            f = figure(plot_width=1000, plot_height=250,
                       x_range=[min_datetime, max_datetime],
                       x_axis_type='datetime')
            draw_fn(r, f)
            columns.append(f)

        result.append(columns)
    return gridplot(result)


def plot_resources_usage(report):
    monitor = report.monitor
    nodes = get_sorted_nodes(report.cluster, ["monitor"])
    data = [(node, monitor[node]) for node in nodes if node in monitor]

    datetimes = pd.concat(frame["datetime"] for (_, frame) in data)

    min_datetime = datetimes.min()
    max_datetime = datetimes.max()

    def generate_row(node, frame):
        yield (node, frame, "cpumem")
        yield (node, frame, "network")
        yield (node, frame, "net-connections")
        yield (node, frame, "io")

    def draw(args, figure):
        node, frame, method = args
        time = frame["datetime"]
        resources = frame["resources"]

        figure.plot_width = 500
        figure.plot_height = 250

        if method != "cpumem":
            figure.plot_width = 400

        def draw_bytes(title, read_col, write_col):
            def accumulate(column):
                values = resources.apply(lambda res: res[column])
                values = values - values.min()
                return resample(values, time)

            read = accumulate(read_col)
            write = accumulate(write_col)

            figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
            figure.line(read.index, read, color="blue", legend_label="{} RX".format(title))
            figure.line(write.index, write, color="red", legend_label="{} TX".format(title))

        if method == "cpumem":
            cpu = resample(resources.apply(lambda res: average(res["cpu"])), time)
            mem = resample(resources.apply(lambda res: res["mem"]), time)

            processes = (process_name(p) for p in report.cluster.nodes[node] if process_name(p))
            figure.title = Title(text="{}: {}".format(node, ",".join(processes)))

            figure.yaxis[0].formatter = NumeralTickFormatter(format="0 %")
            figure.line(cpu.index, cpu / 100.0, color="blue", legend_label="CPU")
            figure.line(mem.index, mem / 100.0, color="red", legend_label="Memory")
        elif method == "network":
            draw_bytes("Net", "net-read", "net-write")
        elif method == "net-connections":
            connections = resample(resources.apply(lambda res: res["connections"]), time)
            figure.line(connections.index, connections, legend_label="Network connections")
        elif method == "io":
            draw_bytes("Disk", "disk-read", "disk-write")

    return plot_time_chart(data, draw, min_datetime=min_datetime, max_datetime=max_datetime, generate_row=generate_row)


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


def create_page(report):
    structure = [
        ("Resources", plot_resources_usage),
        ("Output", plot_output)
    ]

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
    page = create_page(report)
    save(page, output, title="Cluster monitor", resources=CDN)


def serve(cluster_file, port):
    class Handler(web.RequestHandler):
        def get(self):
            report = load_report(cluster_file)
            page = create_page(report)
            self.write(file_html(page, CDN, "Cluster report"))

    app = web.Application([
        (r"/", Handler),
    ])
    app.listen(port)

    print(f"Serving report at http://0.0.0.0:{port}")
    ioloop.IOLoop.current().start()
