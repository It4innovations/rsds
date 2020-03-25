import os
import re

import networkx
from postprocessing.trace import parse_trace
from tqdm import tqdm


def trace_to_networkx(trace_path):
    graph = networkx.DiGraph()

    def handle_event(timestamp, fields, tasks, workers):
        action = fields["action"]
        if action == "task":
            task_id = fields["task"]
            event = fields.get("event")
            if event == "create":
                inputs = tuple(int(i) for i in fields.get("inputs", "").rstrip(",").split(",") if i)

                graph.add_node(task_id, duration=0, size=fields.get("size"))
                graph.add_edges_from((inp, task_id) for inp in inputs)
            elif event == "finish":
                start = fields["start"]
                end = fields["stop"]
                duration = end - start
                graph.nodes[task_id]["duration"] = duration
                graph.nodes[task_id]["size"] = fields["size"]

    parse_trace(trace_path, handle_event)
    return graph


def avg(values):
    return sum(values) / len(values)


def median(values):
    values = sorted(values)
    half = len(values) // 2
    if len(values) % 2 == 0:
        return (values[half - 1] + values[half]) / 2
    else:
        return values[half]


KiB = 1024


def task_graph_table():
    DIR = "/mnt/salomon/projects/rsds/experiments/runs/graph-info"
    usecases = {
        "pandas_groupby-360-1s-1H": "D",
        "pandas_groupby-360-1s-8H": "D",
        "pandas_join-1-1s-1T": "D",
        "pandas_join-1-1s-1H": "D",
        "pandas_join-1-2s-1H": "D",
        "bag-25000-10": "B",
        "bag-25000-50": "B",
        "bag-25000-100": "B",
        "bag-25000-200": "B",
        "merge-10000": "F",
        "merge-25000": "F",
        "merge-50000": "F",
        "numpy-50000-10": "A",
        "numpy-50000-50": "A",
        "numpy-50000-100": "A",
        "numpy-50000-200": "A",
        "tree-15": "F",
        "xarray-5": "X",
        "xarray-25": "X",
        "wordbatch_vectorizer-wordbatch.csv-100000": "F",
        "wordbatch_wordbag-wordbatch.csv-100000": "F"
    }
    renames = {
        re.compile(r"^pandas.*"): lambda s: s[7:],
        re.compile(r"^wordbatch_.*"): lambda s: s[10:].replace("wordbatch.csv-", "")
    }

    table = r"""\begin{table}
    \caption{Task graph properties}
    \centering
    \label{tab:graph_properties}
\begin{tabular}{l|rrrrrc}
    \toprule
    Graph & \#T & \#I & S & D & LP & API \\
    \midrule
"""
    for (name, api) in tqdm(usecases.items()):
        trace_path = None
        for dir in os.listdir(DIR):
            potential_trace = os.path.join(DIR, dir, "scheduler.trace")
            if dir.endswith(f"{name}-0") and os.path.isfile(potential_trace):
                trace_path = potential_trace
                break
        if trace_path is None:
            print(f"Warning: {name} not found, skipping")
            continue
        print(name, trace_path)

        for (regex, transformer) in renames.items():
            if regex.match(name):
                orig_name = name
                name = transformer(name)
                print(f"Renamed {orig_name} to {name}")
                break

        g = trace_to_networkx(trace_path)
        node_count = len(g.nodes)
        edge_count = len(g.edges)

        sizes = networkx.get_node_attributes(g, "size").values()
        avg_size = f"{avg(sizes) / KiB:.3f}"

        durations = networkx.get_node_attributes(g, "duration").values()
        avg_duration = f"{avg(durations) / 1000:.3f}"

        longest_path = networkx.dag_longest_path_length(g)

        table += f"{name} & {node_count} & {edge_count} & {avg_size} & {avg_duration} & {longest_path} & {api} \\\\\n"
    table += r"""\bottomrule
    \end{tabular}\\
    \vspace{1mm}
    \#T = Number of tasks; \#I = Number of dependencies; \\
    S = Average task output size [KiB]; D = Average task duration [ms]; \\
    LP = longest oriented path in the graph; \\
    D = DataFrame; B = Bag; A = Arrays; F = Futures; X = XArray
\end{table}"""
    print(table)


task_graph_table()
