import datetime
import functools
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time
import traceback
from random import Random

import click
import pandas as pd
import psutil
import seaborn as sns
import tqdm
from distributed import Client
from orco import cfggen
from usecases import bench_numpy, bench_dataframe, bench_bag, bench_merge, bench_tree, bench_xarray

BUILD_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent

USECASES = {
    "xarray": bench_xarray,
    "tree": bench_tree,
    "bag": bench_bag,
    "numpy": bench_numpy,
    "merge": bench_merge,
    "dataframe": bench_dataframe
}


class Process:
    def __init__(self, args, node="localhost", remote=False, env=None, workdir=None, tag=None):
        file_path = os.path.join(workdir, tag)

        environment = os.environ.copy()
        if env is not None:
            environment.update(env)
        environment["RUST_BACKTRACE"] = "full"

        self.args = args
        self.command = f"""
source ~/.bashrc
workon dask
cd /tmp
{" ".join(args)} > {file_path}.out 2> {file_path}.err &
ps -ho pgid $!
""".strip()
        args = []
        if remote:
            args += ["ssh", node]
        else:
            args += ["setsid"]
        args += ["/bin/bash"]

        logging.debug(f"Starting {'remote' if remote else 'local'} process: {self.command}")
        self.node = node
        self.remote = remote
        self.process = subprocess.Popen(args,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        env=environment,
                                        cwd=workdir)
        out, err = self.process.communicate(self.command.encode())
        self.pid = int(out.decode().strip())
        if not self.pid:
            raise Exception(f"Could not start process {self.args}: {out.decode()}, {err.decode()}")
        logging.debug(f"Started {self.args} at {self.node}: {self.pid}")

    def kill(self):
        args = ["kill", "-TERM", f"-{self.pid}"]
        if self.remote:
            args = ["ssh", self.node, "--"] + args
        res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        if res.returncode != 0:
            logging.error(f"Could not kill process {self.args} ({self.pid}) at {self.node}: {res.stdout}, {res.stderr}")


class Cluster:
    def __init__(self, cluster_info, workdir, port=None):
        if port is None:
            port = generate_port()

        self.scheduler = cluster_info["scheduler"].replace("$BUILD", str(BUILD_DIR))
        self.workers = cluster_info["workers"]

        self.port = port

        self.scheduler_address = f"{socket.gethostname()}:{port}"
        self.scheduler_process = Process((self.scheduler, "--port", str(port)), workdir=workdir, tag="scheduler")
        logging.info(f"Starting a cluster at {self.scheduler_address}, workers: {self.workers}")

        self.worker_processes = self._start_workers(self.workers, self.scheduler_address, workdir)

        self.client = Client(f"tcp://{self.scheduler_address}", timeout=60)
        self.client.wait_for_workers(self.workers["count"])

    def kill(self):
        for process in self.worker_processes:
            process.kill()
        self.client.close()
        self.scheduler_process.kill()

    def name(self):
        return f"{self.scheduler}/{self.workers}"

    def _start_workers(self, workers, scheduler_address, workdir):
        count = workers["count"]
        cores = workers["cores"]

        def get_args(cores):
            return ("dask-worker", scheduler_address, "--nthreads", "1", "--nprocs", str(cores))

        env = os.environ.copy()
        env["OMP_NUM_THREADS"] = "1"  # TODO

        if count == 1:
            return [Process(get_args(cores), env=env, workdir=workdir, tag="worker-0")]
        else:
            nodes = get_pbs_nodes()
            if count >= len(nodes):
                raise Exception("Requesting more nodes than got from PBS (one is reserved for scheduler and client)")
            return [Process(get_args(cores), node=node, env=env, remote=True, workdir=workdir, tag=f"worker-{i}") for
                    i, node in enumerate(nodes[1:])]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kill()


class Benchmark:
    def __init__(self, config, workdir):
        self.clusters = config.get("clusters", ())
        self.usecases = tuple(self._gen_usecases(config.get("usecases", ())))
        self.repeat = config.get("repeat", 1)
        self.workdir = os.path.join(workdir, "work")
        os.makedirs(self.workdir, exist_ok=True)

    def run(self):
        results = [benchmark_cluster(cluster, self.usecases, self.repeat, self.workdir) for cluster in self.clusters]
        clusters = [format_cluster_info(c) for c in self.clusters]
        check_results(results, clusters)
        return create_frame(results, clusters)

    def _gen_usecases(self, usecases):
        for item in usecases:
            for variant in item:
                function = variant["usecase"]
                args = variant["args"]
                name = f"{function}/{args}"
                yield (name, functools.partial(USECASES[function], args))

    def __len__(self):
        return len(self.usecases)


def check_free_port(port):
    assert isinstance(port, int)
    for conn in psutil.net_connections('tcp'):
        if conn.laddr.port == port and conn.status == "LISTEN":
            return False
    return True


def generate_port():
    random = Random()
    while True:
        port = random.randrange(2000, 65000)
        if check_free_port(port):
            return port


def get_pbs_nodes():
    if "PBS_NODEFILE" not in os.environ:
        raise Exception("Not in a PBS job!")

    with open(os.environ["PBS_NODEFILE"]) as f:
        return [line.strip() for line in f]


def format_cluster_info(cluster_info):
    workers = cluster_info['workers']
    workers = f"{workers['count']}/{workers['cores']}"
    scheduler = cluster_info['scheduler']
    return f"{os.path.basename(scheduler)}/{workers}"


def check_results(results, clusters):
    reference = 0

    for cluster, result in zip(clusters, results):
        for (fn, data) in result.items():
            ref = results[reference][fn]
            if len(set(data["results"])) != 1:
                raise Exception(f"Inconsistent result for {cluster}/{fn}: {data['results']}")

            if data["results"] != ref["results"]:
                raise Exception(f"Wrong result for {cluster}/{fn} (expected {ref['results']}, got {data['results']})")


def create_frame(results, clusters):
    frame = {
        "function": [],
        "cluster": [],
        "time": []
    }

    for cluster, result in zip(clusters, results):
        for (fn, data) in result.items():
            frame["cluster"] += [cluster] * len(data["times"])
            frame["function"] += [fn] * len(data["times"])
            frame["time"] += data["times"]
    return pd.DataFrame(frame)


def gen_results(cluster_info, usecases, repeat, workdir):
    for (name, function) in usecases:
        for i in range(repeat):
            logging.info(f"Benchmarking {name} ({i})")
            with Cluster(cluster_info, workdir):
                start = time.time()
                result = function()
                duration = time.time() - start
                yield (name, result, duration)


def benchmark_cluster(cluster_info, usecases, repeat, workdir):
    logging.info(f"Benchmarking {cluster_info}")
    results = {name: {
        "results": [],
        "times": []
    } for (name, _) in usecases}
    try:
        for (function, result, duration) in tqdm.tqdm(gen_results(cluster_info, usecases, repeat, workdir),
                                                      total=len(usecases) * repeat):
            results[function]["results"].append(result)
            results[function]["times"].append(duration)
    except:
        print(f"Error while processing {cluster_info}", file=sys.stderr)
        traceback.print_exc()
    return results


def create_boxplot(frame):
    def extract(fn):
        name, variant = fn.split("/")
        return (name, int(variant))

    clusters = sorted(set(frame["cluster"]))
    functions = sorted(set(frame["function"]), key=extract)

    def plot(data, **kwargs):
        sns.boxplot(x=data["cluster"], y=data["time"] * 1000, order=clusters)

    g = sns.FacetGrid(frame, col="function", col_wrap=4, col_order=functions, sharey=False)
    g = g.map_dataframe(plot)
    g = g.add_legend()
    g.set_ylabels("Time [ms]")
    g.set(ylim=(0, None))
    return g


def save_results(frame, directory):
    os.makedirs(directory, exist_ok=True)
    frame.to_json(os.path.join(directory, "result.json"))
    plot = create_boxplot(frame)
    plot.savefig(os.path.join(directory, "result.png"))

    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.expand_frame_repr', False):
        with open(os.path.join(directory, "summary.txt"), "w") as f:
            s = frame.groupby(["function", "cluster"]).describe()
            f.write(f"{s}\n")
            s = frame.groupby(["cluster"]).describe()
            f.write(f"{s}\n")


@click.command()
@click.argument("input")
@click.argument("output-dir")
def benchmark(input, output_dir):
    output_dir = os.path.abspath(output_dir)
    benchmark = Benchmark(cfggen.build_config_from_file(input), output_dir)
    frame = benchmark.run()
    save_results(frame, output_dir)


@click.command()
@click.argument("input")
@click.option("--name", default=None)
@click.option("--nodes", default=8)
@click.option("--queue", default="qexp")
@click.option("--walltime", default="01:00:00")
@click.option("--project", default="")
@click.option("--workdir", default="runs")
def submit(input, name, nodes, queue, walltime, workdir, project):
    if name is None:
        name = f"{datetime.datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}"

    if project:
        project = f"#PBS -A {project}"

    input = os.path.abspath(input)
    directory = os.path.join(os.path.abspath(workdir), name)
    os.makedirs(directory, exist_ok=True)
    stdout = os.path.join(directory, "stdout")
    stderr = os.path.join(directory, "stderr")

    shutil.copyfile(input, os.path.join(directory, os.path.basename(input)))
    script_path = os.path.abspath(__file__)

    command = f"""#!/bin/bash

#PBS -l select={nodes},walltime={walltime}
#PBS -q {queue}
#PBS -o {stdout}
#PBS -e {stderr}
{project}

source ~/.bashrc || exit 1
workon dask || exit 1

python {script_path} benchmark {input} {directory}"""
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
