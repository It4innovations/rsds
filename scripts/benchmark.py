import datetime
import functools
import itertools
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
from random import Random

import click
import dask
import numpy as np
import pandas as pd
import psutil
import seaborn as sns
import tqdm
import xarray
from distributed import Client
from monitor.src.cluster import start_process, HOSTNAME, kill_process, CLUSTER_FILENAME, Cluster
from orco import cfggen
from usecases import bench_numpy, bench_dataframe, bench_bag, bench_merge, bench_merge_slow, bench_tree, bench_xarray

CURRENT_DIR = pathlib.Path(os.path.abspath(__file__)).parent
BUILD_DIR = CURRENT_DIR.parent

TIMEOUT_EXIT_CODE = 16
RESULT_FILE = "result.json"
DEFAULT_VENV = "dask"

USECASES = {
    "xarray": bench_xarray,
    "tree": bench_tree,
    "bag": bench_bag,
    "numpy": bench_numpy,
    "merge": bench_merge,
    "merge_slow": bench_merge_slow,
    "dataframe": bench_dataframe
}


class DaskCluster:
    def __init__(self, cluster_info, workdir, port=None, profile=False):
        if port is None:
            port = generate_port()

        self.scheduler = cluster_info["scheduler"]
        self.workers = cluster_info["workers"]

        venv = self.scheduler.get("venv", DEFAULT_VENV)
        self.workdir = workdir
        self.port = port
        self.profile = profile
        self.init_cmd = (
            "source ~/.bashrc",
            f"workon {venv}"
        )

        self.scheduler_address = f"{HOSTNAME}:{port}"
        self.cluster = Cluster(self.workdir)

        logging.info(f"Starting {format_cluster_info(cluster_info)} at {self.scheduler_address}")
        self._start_scheduler(self.scheduler)
        self._start_workers(self.workers, self.scheduler_address)
        if self._use_monitoring():
            self._start_monitors()

        with open(os.path.join(self.workdir, CLUSTER_FILENAME), "w") as f:
            self.cluster.serialize(f)

        self.client = Client(f"tcp://{self.scheduler_address}", timeout=30)
        self.client.wait_for_workers(worker_count(self.workers["count"]))

    def start(self, args, name, host=None, env=None, workdir=None):
        if workdir is None:
            workdir = self.workdir

        pid, cmd = start_process(args, host=host, workdir=workdir, name=name, env=env, init_cmd=self.init_cmd)
        self.cluster.add(host if host else HOSTNAME, pid, cmd, key=name)

    def kill(self):
        self.client.close()

        def kill_fn(node, process):
            signal = "TERM"
            if process["key"].startswith("scheduler") and self._profile_flamegraph():
                signal = "INT"

            assert kill_process(node, process["pid"], signal=signal)

        self.cluster.kill(kill_fn)

    def name(self):
        return f"{self.scheduler}-{self.workers}"

    def _start_scheduler(self, scheduler):
        binary = scheduler["binary"].replace("$BUILD", str(BUILD_DIR))

        args = [binary, "--port", str(self.port)] + list(scheduler.get("args", ()))
        if self._trace_scheduler():
            args += ["--trace-file", os.path.join(self.workdir, "scheduler.trace")]

        if self._profile_flamegraph() and "rsds" in self.scheduler:
            args = ["flamegraph", "-o", os.path.join(self.workdir, "scheduler.svg"), "--"] + args

        self.start(args, name="scheduler", env={
            "RUST_BACKTRACE": "full"
        })

    def _start_workers(self, workers, scheduler_address):
        count = workers["count"]
        cores = workers["cores"]

        def get_args(cores):
            return ("dask-worker", scheduler_address, "--nthreads", "1", "--nprocs", str(cores), "--no-dashboard")

        env = {"OMP_NUM_THREADS": "1"}  # TODO

        if count == "local":
            self.start(get_args(cores), env=env, name="worker-0", workdir="/tmp")
        else:
            nodes = get_pbs_nodes()
            if count >= len(nodes):
                raise Exception("Requesting more nodes than got from PBS (one is reserved for scheduler and client)")
            for i, node in zip(range(count), nodes[1:]):
                self.start(get_args(cores), host=node, env=env, name=f"worker-{i}", workdir="/tmp")

    def _start_monitors(self):
        monitor_script = os.path.join(CURRENT_DIR, "monitor", "monitor.py")

        def start(node=None):
            path = os.path.join(self.workdir, f"monitor-{node if node else HOSTNAME}.trace")
            self.start(("python", monitor_script, path), host=node, name="monitor")

        if is_inside_pbs():
            for node in get_pbs_nodes():
                start(node)
        else:
            start()

    def _profile_flamegraph(self):
        return "flamegraph" in self.profile

    def _use_monitoring(self):
        return "monitor" in self.profile

    def _trace_scheduler(self):
        return "trace" in self.profile

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kill()


class Benchmark:
    def __init__(self, config, workdir, profile):
        self.configurations = tuple(self._gen_configurations(config.get("configurations", ())))
        self.repeat = config.get("repeat", 1)
        self.reference = config.get("reference")
        self.workdir = workdir
        self.profile = profile

    def run(self, timeout, bootstrap):
        os.makedirs(self.workdir, exist_ok=True)

        if bootstrap is not None:
            bootstrap = pd.read_json(bootstrap)
        else:
            bootstrap = pd.DataFrame()

        results, timeouted = self.benchmark_configurations(timeout, bootstrap)

        try:
            check_results(results, self.reference)
        except:
            logging.error("Result check failed")
            traceback.print_exc()
        return pd.concat((bootstrap, results), ignore_index=True, sort=False), timeouted

    def benchmark_configurations(self, timeout, bootstrap):
        def repeat_configs():
            for config in self.configurations:
                for i in range(self.repeat):
                    c = dict(config)
                    c["index"] = i
                    yield c

        results = results = {
            "cluster": [],
            "function": [],
            "result": [],
            "time": [],
            "index": []
        }
        configurations = skip_completed(list(repeat_configs()), bootstrap)

        def run():
            for configuration in tqdm.tqdm(configurations):
                repeat_index = configuration["index"]
                logging.info(
                    f"Benchmarking {configuration['name']} on {format_cluster_info(configuration['cluster'])} ({repeat_index})")

                try:
                    configuration, result, duration = self.benchmark_configuration(configuration, repeat_index)

                    results["cluster"].append(format_cluster_info(configuration["cluster"]))
                    results["function"].append(configuration["name"])
                    results["result"].append(result)
                    results["time"].append(duration)
                    results["index"].append(repeat_index)
                except KeyboardInterrupt:
                    break
                except:
                    print(f"Error while processing {configuration}", file=sys.stderr)
                    traceback.print_exc()

        timeouted = False
        if timeout is None:
            run()
        else:
            t = threading.Thread(target=run, daemon=True)
            t.start()
            t.join(timeout)
            if t.is_alive():
                logging.warning(f"Benchmark did not finish in {timeout} seconds")
                timeouted = True

        return pd.DataFrame(results), timeouted

    def benchmark_configuration(self, configuration, index):
        workdir = os.path.join(self.workdir,
                               f"{format_cluster_info(configuration['cluster'])}-{configuration['name']}-{index}")
        os.makedirs(workdir, exist_ok=True)

        with DaskCluster(configuration["cluster"], workdir, profile=self.profile):
            start = time.time()
            # the real computation happens here
            result = dask.compute(configuration["function"]())
            duration = time.time() - start
            return (configuration, flatten_result(result), duration)

    def _gen_configurations(self, configurations):
        for configuration in configurations:
            usecase = configuration["usecase"]
            function = usecase["function"]
            args = usecase["args"]
            if not isinstance(args, (list, tuple)):
                args = (args, )

            name = f"{function}-{'-'.join((str(arg) for arg in args))}"
            yield {
                "name": name,
                "function": functools.partial(USECASES[function], *args),
                "cluster": configuration["cluster"]
            }


def skip_completed(configurations, bootstrap):
    if len(bootstrap) > 0:
        filtered_configs = []

        def create_key(config):
            return (format_cluster_info(config["cluster"]), config["name"], config["index"])

        grouped = bootstrap.groupby(["cluster", "function", "index"], sort=False)
        for config in configurations:
            if create_key(config) not in grouped.groups:
                filtered_configs.append(config)
        logging.info(
            f"Skipping {len(configurations) - len(filtered_configs)} out of {len(configurations)} configurations")
        return filtered_configs
    else:
        return configurations


def flatten_result(res):
    if isinstance(res, xarray.DataArray):
        return flatten_result(res.values)
    elif isinstance(res, np.ndarray):
        return flatten_result(res.ravel()[0])
    elif isinstance(res, (list, tuple)):
        return flatten_result(res[0])
    else:
        return res


def worker_count(workers):
    if workers == "local":
        return 1
    return workers


def check_free_port(port):
    assert isinstance(port, int)
    for conn in psutil.net_connections('tcp'):
        if conn.laddr.port == port:
            return False
    return True


def generate_port():
    random = Random()
    while True:
        port = random.randrange(2000, 65000)
        if check_free_port(port):
            return port


def is_inside_pbs():
    return "PBS_NODEFILE" in os.environ


def get_pbs_nodes():
    assert is_inside_pbs()

    with open(os.environ["PBS_NODEFILE"]) as f:
        return [line.strip() for line in f]


def format_cluster_info(cluster_info):
    workers = cluster_info['workers']
    workers = f"{workers['count']}-{workers['cores']}"
    scheduler = cluster_info['scheduler']['name']
    return f"{scheduler}-{workers}"


def check_results(frame, reference):
    clusters = list(frame["cluster"].unique())
    functions = list(frame["function"].unique())

    # self consistency
    for (cluster, function) in itertools.product(clusters, functions):
        results = list(frame[(frame["cluster"] == cluster) & (frame["function"] == function)]["result"])
        if len(set(results)) > 1:
            raise Exception(f"Inconsistent result for {cluster}/{function}: {results}")

    # reference equality
    if reference:
        for (cluster, function) in itertools.product(clusters, functions):
            results = list(frame[(frame["cluster"] == cluster) & (frame["function"] == function)]["result"])
            if len(results) == 0:
                continue

            result = results[0]
            for cl in clusters:
                if reference in cl:
                    ref_results = list(frame[(frame["cluster"] == cl) & (frame["function"] == function)]["result"])
                    if len(ref_results) == 0:
                        continue
                    if ref_results[0] != result:
                        raise Exception(
                            f"Wrong result for {cluster}/{function} (expected {ref_results[0]}, got {result})")


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


def save_results(frame, directory):
    def plot_box(data, clusters, **kwargs):
        sns.boxplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    def plot_scatter(data, clusters, **kwargs):
        sns.swarmplot(x=data["cluster"], y=data["time"] * 1000, hue=data["cluster"], order=clusters, hue_order=clusters)

    os.makedirs(directory, exist_ok=True)
    frame.to_json(os.path.join(directory, RESULT_FILE))

    if len(frame) > 0:
        for (file, plot_fn) in (
                ("result_boxplot", plot_box),
                ("result_scatterplot", plot_scatter)
        ):
            plot = create_plot(frame, plot_fn)
            plot.savefig(os.path.join(directory, f"{file}.png"))

    def describe(frame, file):
        if len(frame) > 0:
            s = frame.groupby(["function", "cluster"]).describe()
            file.write(f"{s}\n")
            s = frame.groupby(["cluster"]).describe()
            file.write(f"{s}\n")
        else:
            file.write("(empty dataframe)")

    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.expand_frame_repr', False):
        with open(os.path.join(directory, "summary.txt"), "w") as f:
            f.write("All results:\n")
            describe(frame, f)
            f.write("Results without first run:\n")
            describe(frame[frame["index"] != 0], f)


def execute_benchmark(input, output_dir, profile, timeout, bootstrap) -> bool:
    output_dir = os.path.abspath(output_dir)
    if bootstrap:
        bootstrap = os.path.abspath(bootstrap)
        assert os.path.isfile(bootstrap)

    benchmark = Benchmark(cfggen.build_config_from_file(input), output_dir, profile)
    frame, timeouted = benchmark.run(timeout, bootstrap)
    frame.drop(columns=["result"], inplace=True)
    save_results(frame, output_dir)
    return timeouted


class WalltimeType(click.ParamType):
    name = "walltime"

    def convert(self, value, param, ctx):
        pattern = r'(((?P<hours>\d+):)?(?P<minutes>\d+):)?(?P<seconds>\d+)'
        m = re.match(pattern, value)
        if m:
            wtime = m.groupdict(default="0")
            return int(wtime["hours"]) * 3600 + int(wtime["minutes"]) * 60 + int(wtime["seconds"])
        elif value.isdigit():
            return int(value)

        self.fail("%s is not valid walltime" % value, param, ctx)


@click.command()
@click.argument("input")
@click.argument("output-dir")
@click.option("--profile", default="")
@click.option("--timeout", default=None, type=int)
@click.option("--bootstrap", default=None)
def benchmark(input, output_dir, profile, timeout, bootstrap):
    if execute_benchmark(input, output_dir, profile.split(","), timeout, bootstrap):
        print("Benchmark timeouted")
        exit(TIMEOUT_EXIT_CODE)


@click.command()
@click.argument("input")
@click.option("--name", default=None)
@click.option("--nodes", default=8)
@click.option("--queue", default="qexp")
@click.option("--walltime", default="01:00:00", type=WalltimeType())
@click.option("--project", default="")
@click.option("--workdir", default="runs")
@click.option("--profile", default="")
@click.option("--bootstrap", default=None)
@click.option("--workon", default=DEFAULT_VENV)
def submit(input, name, nodes, queue, walltime, workdir, project, profile, bootstrap, workon):
    if name is None:
        actual_name = f"{datetime.datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}"
    else:
        actual_name = name

    if project:
        pbs_project = f"#PBS -A {project}"
    else:
        pbs_project = ""

    input = os.path.abspath(input)
    assert os.path.isfile(input)

    # sanity check
    Benchmark(cfggen.build_config_from_file(input), workdir, profile)

    directory = os.path.join(os.path.abspath(workdir), actual_name)
    os.makedirs(directory, exist_ok=True)
    stdout = os.path.join(directory, "stdout")
    stderr = os.path.join(directory, "stderr")

    shutil.copyfile(input, os.path.join(directory, os.path.basename(input)))
    script_path = os.path.abspath(__file__)
    args = ["--timeout", str(walltime - 60)]  # lower timeout to save results
    if profile:
        args += ["--profile", profile]
    if bootstrap:
        bootstrap = os.path.abspath(bootstrap)
        assert os.path.isfile(bootstrap)
        args += ["--bootstrap", bootstrap]

    command = f"""#!/bin/bash
#PBS -l select={nodes},walltime={walltime}
#PBS -q {queue}
#PBS -N {actual_name}
#PBS -o {stdout}
#PBS -e {stderr}
{pbs_project}

source ~/.bashrc || exit 1
workon {workon} || exit 1

python {script_path} benchmark {input} {directory} {" ".join(args)}
if [ $? -eq {TIMEOUT_EXIT_CODE} ]
then
    cd ${{PBS_O_WORKDIR}}
    python {script_path} submit \
{f"--name {name}" if name else ""} \
--nodes {nodes} \
--queue {queue} \
--walltime {walltime} \
{f"--project {project}" if project else ""} \
--workdir {workdir} \
--bootstrap {os.path.join(directory, RESULT_FILE)} \
--workon {workon} \
{f"--profile {profile}" if profile else ""} \
{input}
fi
"""
    fpath = f"/tmp/pbs-{name}.sh"
    with open(fpath, "w") as f:
        f.write(command)

    print(f"Submitting PBS script: {fpath}")
    result = subprocess.run(["qsub", fpath], stdout=subprocess.PIPE)
    print(f"Job id: {result.stdout.decode().strip()}")


@click.group()
def cli():
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cli.add_command(benchmark)
    cli.add_command(submit)
    cli()
