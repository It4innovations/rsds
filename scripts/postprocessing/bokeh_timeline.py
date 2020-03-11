import collections
import math

import pandas as pd
from bokeh.models import ColumnDataSource, HoverTool

TaskStartTraceEvent = collections.namedtuple("TaskStart", ["time", "worker", "task"])
TaskEndTraceEvent = collections.namedtuple("TaskEnd", ["time", "worker", "task"])


def merge_trace_events(trace_events, start_pred, end_pred, key_fn, start_map=None, end_map=None):
    """
    Produces a stream of matched starts and ends of events in the form of
    merge_fn(start_event, end_event).
    """
    open_events = {}

    for event in trace_events:
        if start_pred(event):
            key = key_fn(event)
            assert key not in open_events
            if start_map:
                open_events[key] = start_map(event)
            else:
                open_events[key] = event
        elif end_pred(event):
            key = key_fn(event)
            start_event = open_events[key]
            if end_map:
                yield end_map(start_event, event)
            else:
                yield start_event, event


# https://stackoverflow.com/a/31631711/1107768
def format_bytes(size):
    """"Return the given bytes as a human friendly KB, MB, GB, or TB string"""
    size = float(size)
    KB = float(1024)
    MB = float(KB ** 2)  # 1,048,576
    GB = float(KB ** 3)  # 1,073,741,824

    if size < KB:
        return '{0} {1}'.format(size, 'B' if 0 == size > 1 else 'B')
    elif KB <= size < MB:
        return '{0:.2f} KiB'.format(size / KB)
    elif MB <= size < GB:
        return '{0:.2f} MiB'.format(size / MB)
    elif GB <= size:
        return '{0:.2f} GiB'.format(size / GB)


def task_tooltip(task) -> str:
    return f"id: {task.id}, key: {task.key}, duration: {task.duration:.3f} s, wait: {task.wait_duration:.3f} s, " \
           f"size: {format_bytes(task.size)}, inputs: {len(task.inputs)}, "\
           f"input_size: {format_bytes(task.input_bytes())}"


def build_task_locations(trace_events, worker):
    slots = []

    def find_slot(height):
        h = 0
        for (index, (start, end)) in enumerate(slots):
            if h + height <= start:
                slots.insert(index, (h, h + height))
                return (h, h + height)
            h = end
        last = slots[-1][1] if slots else 0
        slots.append((last, last + height))
        return (last, last + height)

    def map_start(event):
        (start, end) = find_slot(event.task.cpus)
        return (event, (start, end))

    def map_end(start_event, end_event):
        event, slot = start_event
        slots.remove(slot)
        return (event.task,
                (event.time, event.time + event.task.duration, slot[0], slot[1]))

    yield from merge_trace_events(
        trace_events,
        lambda t: isinstance(t, TaskStartTraceEvent) and t.worker == worker,
        lambda t: isinstance(t, TaskEndTraceEvent) and t.worker == worker,
        lambda e: e.task,
        map_start,
        map_end
    )


def plot_tasks_on_workers(trace_events, workers):
    from bokeh import plotting

    end_time = math.ceil(max((e.time for e in trace_events), default=0))

    tools_to_show = 'hover,box_zoom,pan,save,reset,wheel_zoom'
    plot = plotting.figure(plot_width=1600, plot_height=850,
                           x_range=(0, end_time),
                           title='Tasks on workers',
                           tools=tools_to_show)
    plot.yaxis.axis_label = 'Worker'
    plot.xaxis.axis_label = 'Time [s]'

    data = collections.defaultdict(lambda: [])
    factor = 0.1
    worker_index = 0

    # render task rectangles
    for index, worker in enumerate(workers):
        locations = list(build_task_locations(trace_events, worker))
        if not locations:
            continue

        def normalize_height(height):
            return height + worker_index

        for (task, rect) in locations:
            data["left"].append(rect[0])
            data["right"].append(rect[1])
            data["bottom"].append(normalize_height(rect[2]) + factor)
            data["top"].append(normalize_height(rect[3]) - factor)
            data["task"].append(task_tooltip(task))
            data["duration"].append(f"{task.duration} s")
            data["worker"].append(f"{worker.id}: {worker.address}")
            data["color"].append("blue")

        plot.line([0, end_time], [worker_index, worker_index], color="black", line_dash="dashed", alpha=0.5,
                  line_width=0.5)
        worker_index += 1

    source = ColumnDataSource(data=data)
    plot.quad(source=source, left="left", right="right", bottom="bottom", top="top", line_color="color",
              fill_color="color")
    hover = plot.select(dict(type=HoverTool))
    hover.tooltips = [("Task", "@task"), ("Duration", "@duration"), ("Worker", "@worker")]
    hover.mode = 'mouse'

    return plot


def plot_task_lifespan(tasks, end_time, packets, task_filter=None):
    from bokeh import plotting

    tools_to_show = 'box_zoom,pan,save,reset,wheel_zoom'
    plot = plotting.figure(plot_width=1600, plot_height=850,
                           x_range=(0, end_time),
                           title='Task lifespan',
                           tools=tools_to_show)
    plot.yaxis.axis_label = 'Task'
    plot.xaxis.axis_label = 'Time [s]'

    events_colors = {
        "create": "green",
        "assign": "purple",
        "send": "yellow",
        "place": "brown",
        "compute-start": "pink",
        "compute-end": "blue",
        "finish": "black",
        "remove": "red"
    }
    data = collections.defaultdict(lambda: [])

    rect_width = 0.005
    rect_height = 0.2
    row_height = 1

    task_height = 0

    for task in sorted(tasks.values(), key=lambda t: t.id):
        if task_filter and task.key not in task_filter:
            continue

        height = task_height
        task_height += row_height
        center_height = height + row_height / 2
        for (event, time, args) in task.events:
            data["task"].append(task_tooltip(task))
            data["time"].append(f"{time} s")

            if "worker" in args:
                data["worker"].append(str(args["worker"].id))
            else:
                data["worker"].append("")
            data["left"].append(time - rect_width / 2)
            data["right"].append(time + rect_width / 2)
            data["bottom"].append(center_height - rect_height / 2)
            data["top"].append(center_height + rect_height / 2)
            data["event"].append(event)
            data["color"].append(events_colors[event])
        plot.line([0, end_time], [task_height, task_height], color="black", line_dash="dotted", alpha=0.2,
                  line_width=0.2)

    source = ColumnDataSource(data=data)
    plot.quad(source=source, left="left", right="right", bottom="bottom", top="top", line_color="color",
              fill_color="color", legend_field="event", name="tasks")

    packets = pd.DataFrame(packets)
    packets["y"] = -1
    packets["color"] = packets["event"].apply(lambda e: "red" if e == "packet-receive" else "blue")
    packets["size_format"] = packets["size"].apply(lambda s: format_bytes(s))

    packets = ColumnDataSource(data=packets)
    plot.circle(source=packets, size=10, x="time", y="y", color="color", fill_color="color", name="packets")

    plot.add_tools(
        HoverTool(tooltips=[("Task", "@task"), ("Event", "@event"), ("Time", "@time"), ("Worker", "@worker")],
                  names=["tasks"]))
    plot.add_tools(
        HoverTool(tooltips=[("Size", "@size_format"), ("Type", "@event"), ("Time", "@time")], names=["packets"]))

    return plot


def plot_tabs(plots):
    from bokeh.models import Panel, Tabs
    return Tabs(tabs=[Panel(child=fn(), title=label)
                      for (fn, label) in plots])


def build_plot(plot_fn, filename):
    import bokeh.io
    bokeh.io.output_file(filename)
    bokeh.io.save(plot_fn())


def render_rectangles(plot, locations, fill_color="blue", line_color="black", label=None):
    left = [r[0] for r in locations]
    right = [r[1] for r in locations]
    bottom = [r[2] for r in locations]
    top = [r[3] for r in locations]

    plot.quad(left=left, right=right, top=top, bottom=bottom, fill_color=fill_color,
              line_color=line_color, line_width=2, legend_label=label or "")


def normalize_task_events(trace_events):
    return sorted(trace_events,
                  key=lambda e: (e.time, 0 if isinstance(e, TaskStartTraceEvent) else 1))
