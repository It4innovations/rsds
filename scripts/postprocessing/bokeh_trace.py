import collections
import math

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


def plot_task_communication(trace_events, workers):
    """
    Plots individual tasks on workers into a grid chart (one chart per worker).
    """
    from bokeh import plotting

    end_time = math.ceil(max((e.time for e in trace_events)))

    plot = plotting.figure(plot_width=1600, plot_height=850,
                           x_range=(0, end_time),
                           title='CPU schedules')
    plot.yaxis.axis_label = 'Worker'
    plot.xaxis.axis_label = 'Time [s]'

    factor = 0.5

    # render task rectangles
    for index, worker in enumerate(workers):
        locations = list(build_task_locations(trace_events, worker))

        def normalize_height(height):
            return height + index * (1 + factor)

        rectangles = [(
            rect[0],
            rect[1],
            normalize_height(rect[2]),
            normalize_height(rect[3]))
            for (task, rect) in locations
        ]

        render_rectangles(plot, rectangles)

        bottom = max(0, (index * (1 + factor)) - factor / 2)
        plot.line([0, end_time], [bottom, bottom])

    return plot


def plot_tabs(trace_events, workers, plot_fns, labels):
    from bokeh.models import Panel, Tabs
    return Tabs(tabs=[Panel(child=fn(trace_events, workers), title=label)
                      for (fn, label) in zip(plot_fns, labels)])


def plot_all(trace_events, workers):
    return plot_tabs(trace_events, workers,
                     [plot_task_communication],
                     ["Tasks"])


def build_trace_html(trace_events, workers, filename, plot_fn):
    import bokeh.io

    trace_events = normalize_events(trace_events)

    plot = plot_fn(trace_events, workers)

    bokeh.io.output_file(filename)
    bokeh.io.save(plot)


def simulator_trace_to_html(trace_events, workers, filename):
    build_trace_html(trace_events, workers, filename, plot_all)


def render_rectangles(plot, locations, fill_color="blue", line_color="black"):
    left = [r[0] for r in locations]
    right = [r[1] for r in locations]
    bottom = [r[2] for r in locations]
    top = [r[3] for r in locations]

    plot.quad(left=left, right=right, top=top, bottom=bottom, fill_color=fill_color,
              line_color=line_color, line_width=2)


def normalize_events(trace_events):
    return sorted(trace_events,
                  key=lambda e: (e.time, 0 if isinstance(e, TaskStartTraceEvent) else 1))
