import datetime
import functools
import hashlib
import itertools
import json
import logging
import os
import pathlib
import re
import shutil
import socket
import subprocess
import threading
import time
import traceback
from multiprocessing import Pool
from random import Random

import click
import dask
import json5
import numpy as np
import pandas as pd
import psutil
import tqdm
import xarray
from distributed import Client
from git import Repo
from monitor.src.cluster import start_process, HOSTNAME, kill_process, CLUSTER_FILENAME, Cluster
from orco import cfggen
from usecases import bench_numpy, bench_pandas_groupby, bench_pandas_join, bench_bag, bench_merge, bench_merge_slow, \
    bench_tree, bench_xarray, bench_wordbatch_vectorizer, bench_wordbatch_wordbag

CURRENT_DIR = pathlib.Path(os.path.abspath(__file__)).parent
BUILD_DIR = CURRENT_DIR.parent
MODULES = ("hwloc/2.0.3-GCC-6.3.0-2.27", "numactl/2.0.12-GCC-6.3.0-2.27")
USECASES_SCRIPT = os.path.join(CURRENT_DIR, "usecases.py")

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
    "pandas_groupby": bench_pandas_groupby,
    "pandas_join": bench_pandas_join,
    "wordbatch_vectorizer": bench_wordbatch_vectorizer,
    "wordbatch_wordbag": bench_wordbatch_wordbag
}
HASHES = {}
GIT_REPOSITORY = Repo(BUILD_DIR)


def start_process_pool(args):
    args, host, workdir, name, env, init_cmd = args
    return start_process(args, host=host, workdir=workdir, name=name, env=env, init_cmd=init_cmd)


def kill_fn(scheduler_sigint, node, process):
    signal = "TERM"
    if (process["key"].startswith("scheduler") and scheduler_sigint) or "monitor" in process["key"]:
        signal = "INT"

    assert kill_process(node, process["pid"], signal=signal)


class DaskCluster:
    def __init__(self, cluster_info, workdir, port=None, profile=False):
        start = time.time()

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
            f"ml {' '.join(MODULES)}",
            f"workon {venv}"
        )

        write_metadata(self.scheduler, self.workdir)

        protocol = self.scheduler.get("protocol", "tcp")
        self.hostname = HOSTNAME if protocol == "tcp" else socket.gethostbyname(socket.gethostname())
        self.scheduler_address = f"{protocol}://{self.hostname}:{port}"
        self.cluster = Cluster(self.workdir)

        self._start_scheduler(self.scheduler)
        nodes = self._start_workers(self.workers, self.scheduler_address)
        if self._use_monitoring():
            nodes = set(nodes) | {self.hostname}
            self._start_monitors(nodes)

        with open(os.path.join(self.workdir, CLUSTER_FILENAME), "w") as f:
            self.cluster.serialize(f)

        self.client = Client(f"{self.scheduler_address}", timeout=30)

        required_workers = worker_count(self.workers)
        self.client.wait_for_workers(required_workers)
        assert len(self.client.scheduler_info()["workers"]) == required_workers

        logging.info(
            f"Starting {format_cluster_info(cluster_info)} at {self.scheduler_address} took {time.time() - start} s")

    def start(self, args, name, host=None, env=None, workdir=None):
        self.start_many([(args, name, host, env, workdir)])

    def start_many(self, processes):
        def normalize_workdir(workdir):
            return workdir if workdir else self.workdir

        pool_args = [(args, host, normalize_workdir(workdir), name, env, self.init_cmd) for
                     (args, name, host, env, workdir) in processes]
        spawned = []
        if len(pool_args) == 1:
            spawned.append(start_process_pool(pool_args[0]))
        else:
            with Pool() as pool:
                for res in pool.map(start_process_pool, pool_args):
                    spawned.append(res)

        for ((pid, cmd), (_, host, _, name, _, _)) in zip(spawned, pool_args):
            self.cluster.add(host if host else self.hostname, pid, cmd, key=name)

    def kill(self):
        start = time.time()
        self.client.close()

        scheduler_sigint = self._profile_flamegraph() or self._trace_scheduler()
        fn = functools.partial(kill_fn, scheduler_sigint)
        self.cluster.kill(fn)
        logging.info(f"Cluster killed in {time.time() - start} seconds")

    def _start_scheduler(self, scheduler):
        binary = normalize_binary(scheduler["binary"])

        args = [binary, "--port", str(self.port)] + list(scheduler.get("args", ()))
        is_rsds = "rsds" in scheduler["name"]
        if self._trace_scheduler() and is_rsds:
            args += ["--trace-file", os.path.join(self.workdir, "scheduler.trace")]

        if self._profile_flamegraph() and is_rsds:
            args = ["flamegraph", "-o", os.path.join(self.workdir, "scheduler.svg"), "--"] + args

        self.start(args, name="scheduler", env={
            "RUST_BACKTRACE": "full"
        })

    def _start_workers(self, workers, scheduler_address):
        node_count = workers["nodes"]
        processes = workers.get("processes", 1)
        threads = workers.get("threads", 1)
        worker_args = workers.get("args", [])

        def get_args():
            return ["dask-worker", scheduler_address,
                    "--nthreads", str(threads),
                    "--nprocs", str(processes),
                    "--local-directory", "/tmp",
                    "--preload", USECASES_SCRIPT,
                    "--no-dashboard"] + worker_args

        env = {"OMP_NUM_THREADS": "1"}  # TODO

        if node_count == "local":
            self.start(get_args(), env=env, name="worker-0", workdir=self.workdir)
            return [self.hostname]
        else:
            nodes = get_pbs_nodes()
            if node_count >= len(nodes):
                raise Exception("Requesting more nodes than got from PBS (one is reserved for scheduler and client)")
            args = []
            nodes_spawned = []
            for i, node in zip(range(node_count), nodes[1:]):
                args.append((get_args(), f"worker-{i}", node, env, self.workdir))
                nodes_spawned.append(node)
            self.start_many(args)
            return nodes_spawned

    def _start_monitors(self, nodes):
        monitor_script = os.path.join(CURRENT_DIR, "monitor", "monitor.py")
        monitor_args = []

        def start(node):
            path = os.path.join(self.workdir, f"monitor-{node}.trace")
            monitor_args.append((("python", monitor_script, path), "monitor", node, None, None))

        if is_inside_pbs():
            for node in get_pbs_nodes():
                if node in nodes:
                    start(node)
        else:
            start(self.hostname)

        self.start_many(monitor_args)

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
        results["submit-id"] = get_submit_id()
        results = pd.concat((bootstrap, results), ignore_index=True, sort=False)

        try:
            check_results(results, self.reference, timeouted)
            logging.info("Result check succeeded")
        except:
            logging.error("Result check failed")
            traceback.print_exc()
        return results, timeouted

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
                    logging.error(f"Error while processing {configuration}")
                    traceback.print_exc()
                    with open(f"{os.path.join(self.workdir, 'ERROR-DURING-COMPUTE.txt')}", "w") as f:
                        f.write(f"""{configuration['name']} - {format_cluster_info(configuration['cluster'])}""")
                        f.write(traceback.format_exc())
                    break

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

        with DaskCluster(configuration["cluster"], workdir, profile=self.profile) as cluster:
            if configuration["needs_client"]:
                result, duration = configuration["function"](cluster.client)
            else:
                graph = configuration["function"]()
                start = time.time()
                # the real computation happens here
                result = dask.compute(graph)
                duration = time.time() - start
            return (configuration, flatten_result(result), duration)

    def _gen_configurations(self, configurations):
        for configuration in configurations:
            usecase = configuration["usecase"]
            function = usecase["function"]
            args = usecase["args"]
            if not isinstance(args, (list, tuple)):
                args = (args,)

            def format_arg(arg):
                if arg is None:
                    return "default"
                if isinstance(arg, str):
                    return os.path.basename(arg)
                return str(arg)

            name = f"{function}-{'-'.join(format_arg(arg) for arg in args)}"
            yield {
                "name": name,
                "function": functools.partial(USECASES[function], *args),
                "needs_client": usecase.get("needs_client", False),
                "cluster": configuration["cluster"]
            }


def get_file_md5(path):
    if path not in HASHES:
        with open(path, "rb") as sched_file:
            HASHES[path] = hashlib.md5(sched_file.read()).hexdigest()
    return HASHES[path]


def write_metadata(scheduler, directory):
    scheduler_binary = normalize_binary(scheduler["binary"])
    metadata = {
        "scheduler-binary": {
            "path": scheduler_binary,
            "md5": get_file_md5(scheduler_binary)
        },
        "git": {
            "branch": GIT_REPOSITORY.active_branch.name,
            "sha": GIT_REPOSITORY.active_branch.commit.hexsha
        }
    }

    with open(os.path.join(directory, "metadata.txt"), "w") as f:
        json.dump(metadata, f, indent=4)


def normalize_binary(binary):
    path = binary.replace("$BUILD", str(BUILD_DIR))
    if not os.path.isfile(path):
        for directory in os.environ.get("PATH", "").split(":"):
            fullpath = os.path.join(directory, path)
            if os.path.isfile(fullpath):
                path = fullpath
                break
        else:
            raise Exception(f"Path {path} not found")
    return path


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
    nodes = workers["nodes"]
    processes = workers["processes"]
    if nodes == "local":
        return processes
    return nodes * processes


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


def get_submit_id():
    if is_inside_pbs():
        return os.environ["PBS_JOBID"]
    else:
        return f"{os.getpid()}-{int(time.time())}"


def format_cluster_info(cluster_info):
    workers = cluster_info['workers']
    workers = f"{workers['nodes']}n-{workers['processes']}p-{workers.get('threads', 1)}t"
    scheduler = cluster_info['scheduler']['name']
    return f"{scheduler}-{workers}"


def check_results(frame, reference, timeouted):
    clusters = list(frame["cluster"].unique())
    functions = list(frame["function"].unique())

    # self consistency
    for (cluster, function) in itertools.product(clusters, functions):
        results = list(frame[(frame["cluster"] == cluster) & (frame["function"] == function)]["result"])
        if len(set(results)) > 1:
            raise Exception(f"Inconsistent result for {cluster}/{function}: {results}")

    # reference equality
    if reference and not timeouted:
        assert any(reference in cl for cl in clusters)
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


def save_results(frame, directory):
    os.makedirs(directory, exist_ok=True)
    frame.to_json(os.path.join(directory, RESULT_FILE))


def execute_benchmark(input, output_dir, profile, timeout, bootstrap) -> bool:
    output_dir = os.path.abspath(output_dir)
    if bootstrap:
        bootstrap = os.path.abspath(bootstrap)
        assert os.path.isfile(bootstrap)

    benchmark = Benchmark(load_config(input), output_dir, profile)
    frame, timeouted = benchmark.run(timeout, bootstrap)
    save_results(frame, output_dir)
    return timeouted


def load_config(input):
    with open(input) as f:
        content = json5.load(f)
        return cfggen.build_config(content)


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
        logging.error("Benchmark timeouted")
        exit(TIMEOUT_EXIT_CODE)


def submit(input, name, nodes, queue, walltime, workdir, project, profile, bootstrap, workon, watch, postprocess):
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
    Benchmark(load_config(input), workdir, profile)

    directory = os.path.join(os.path.abspath(workdir), actual_name)
    os.makedirs(directory, exist_ok=True)
    stdout = os.path.join(directory, "stdout")
    stderr = os.path.join(directory, "stderr")

    target_input = os.path.join(directory, os.path.basename(input))
    if input != target_input:
        shutil.copyfile(input, target_input)
    script_path = os.path.abspath(__file__)
    args = ["--timeout", str(walltime - 60)]  # lower timeout to save results
    if profile:
        args += ["--profile", profile]
    if bootstrap:
        bootstrap = os.path.abspath(bootstrap)
        assert os.path.isfile(bootstrap)
        args += ["--bootstrap", bootstrap]

    postprocess_cmd = f"python {CURRENT_DIR / 'postprocess.py'} all {directory}" if postprocess else ":"
    command = f"""#!/bin/bash
#PBS -l select={nodes},walltime={walltime}
#PBS -q {queue}
#PBS -N {actual_name}
#PBS -o {stdout}
#PBS -e {stderr}
{pbs_project}

source ~/.bashrc || exit 1
ml {' '.join(MODULES)} || exit 1
workon {workon} || exit 1

python {script_path} benchmark {target_input} {directory} {" ".join(args)}
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
{target_input}
else
    {postprocess_cmd}
fi
"""
    fpath = f"/tmp/pbs-{name}-{int(time.time())}.sh"
    with open(fpath, "w") as f:
        f.write(command)

    print(f"Submitting PBS script: {fpath}")
    result = subprocess.run(["qsub", fpath], stdout=subprocess.PIPE)
    job_id = result.stdout.decode().strip()
    print(f"Job id: {job_id}")

    if watch:
        subprocess.run(["watch", "-n", "10",
                        f"check-pbs-jobs --jobid {job_id} --print-job-err --print-job-out | tail -n 40"])


@click.command("submit")
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
@click.option("--watch/--no-watch", default=False)
@click.option("--postprocess/--no-postprocess", default=True)
def submit_cmd(input, name, nodes, queue, walltime, workdir, project, profile, bootstrap, workon, watch, postprocess):
    submit(input, name, nodes, queue, walltime, workdir, project, profile, bootstrap, workon, watch, postprocess)


@click.group()
def cli():
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cli.add_command(benchmark)
    cli.add_command(submit_cmd)
    cli()
