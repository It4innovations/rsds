import os
import re

import networkx
from postprocessing.trace import parse_trace
from tqdm import tqdm
from multiprocessing import Pool


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


DIR = "/mnt/salomon/projects/rsds/experiments/runs/graph-info"
KiB = 1024
RENAMES = [
    lambda s: re.sub(r"^(pandas_)(.*)", r"\2", s),
    lambda s: re.sub(r"^(wordbatch_)(.*)", r"\2", s),
    lambda s: re.sub(r"(.*)(wordbatch.csv-)(.*)", r"\1\3", s),
    lambda s: re.sub(r"(.*)(000000)(-|$)(.*)", r"\1M\3\4", s),
    lambda s: re.sub(r"(.*)(000)(-|$)(.*)", r"\1K\3\4", s)
]
APIS = {
    "pandas": "D",
    "bag": "B",
    "merge": "F",
    "numpy": "A",
    "tree": "F",
    "xarray": "X",
    "wordbatch": "F"
}


def worker_parse_trace(args):
    name = args

    trace_path = None
    for dir in os.listdir(DIR):
        potential_trace = os.path.join(DIR, dir, "scheduler.trace")
        if dir.endswith(f"{name}-0") and os.path.isfile(potential_trace):
            trace_path = potential_trace
            break
    if trace_path is None:
        print(f"Warning: {name} not found, skipping")
        return None
    print(name, trace_path)
    api = None
    for prefix, apiname in APIS.items():
        if name.startswith(prefix):
            api = apiname
            break
    assert api

    for regex in RENAMES:
        orig_name = name
        name = regex(name)
        if name != orig_name:
            print(f"Renamed {orig_name} to {name}")

    name = name.replace("_", r"\_")
    g = trace_to_networkx(trace_path)
    node_count = len(g.nodes)
    edge_count = len(g.edges)

    sizes = networkx.get_node_attributes(g, "size").values()
    avg_size = f"{avg(sizes) / KiB:.2f}"

    durations = networkx.get_node_attributes(g, "duration").values()
    avg_duration = f"{avg(durations) / 1000:.2f}"

    longest_path = networkx.dag_longest_path_length(g)

    return f"{name} & {node_count} & {edge_count} & {avg_size} & {avg_duration} & {longest_path} & {api} \\\\\n"


def task_graph_table():
    usecases = [
        "bag-25000-10",
        "bag-25000-100",
        "bag-25000-200",
        "bag-25000-50",
        "bag-50000-50",
        "merge-10000",
        "merge-15000",
        "merge-20000",
        "merge-25000",
        "merge-30000",
        "merge-50000",
        "merge_slow-5000-0.1",
        "numpy-50000-10",
        "numpy-50000-100",
        "numpy-50000-200",
        "numpy-50000-50",
        "pandas_groupby-1440-1s-1H",
        "pandas_groupby-1440-1s-8H",
        "pandas_groupby-360-1s-1H",
        "pandas_groupby-360-1s-8H",
        "pandas_groupby-90-1s-1H",
        "pandas_groupby-90-1s-8H",
        "pandas_join-1-1s-1H",
        "pandas_join-1-1s-1T",
        "pandas_join-1-2s-1H",
        "tree-15",
        "wordbatch_vectorizer-wordbatch.csv-1000000-300",
        "wordbatch_wordbag-wordbatch.csv-100000-50",
        "xarray-25",
        "xarray-5"
    ]
    table = r"""\begin{table}
    \caption{Task graph properties}
    \centering
    \label{tab:graph_properties}
\begin{tabular}{l|rrrrrc}
    \toprule
    \textbf{Graph} & \textbf{\#T} & \textbf{\#I} & \textbf{S} & \textbf{AD} & \textbf{LP} & \textbf{API} \\
    \midrule
"""
    with Pool() as pool:
        for line in tqdm(pool.imap(worker_parse_trace, usecases)):
            if line:
                table += line
    table += r"""\bottomrule
    \end{tabular}\\
    \vspace{1mm}
    \#T = Number of tasks; \#I = Number of dependencies; \\
    S = Average task output size [KiB]; AD = Average task duration [ms]; \\
    LP = longest oriented path in the graph; \\
    D = DataFrame; B = Bag; A = Arrays; F = Futures; X = XArray
\end{table}"""
    print(table)


task_graph_table()
