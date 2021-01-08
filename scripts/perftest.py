import json
import os
import shutil
import stat
import subprocess
import time
import logging

import click
from git import Repo

from benchmark import submit, execute_benchmark, DEFAULT_VENV
from orco import cfggen
from postprocess import generate_all
from contextlib import contextmanager

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
BUILD_DIR = os.path.join(ROOT_DIR, "target/release")

REPO = Repo(ROOT_DIR)


@contextmanager
def git_stash():
    head = REPO.head.reference
    print(f"Stashing {head}")
    try:
        REPO.git.stash()
        yield
    finally:
        print(f"Checking out {head} from {REPO.head.reference} and restoring stash")
        REPO.git.checkout(head)
        REPO.git.stash("pop")


# 0d40c8432f2a3830fe36b8f7b498a9e9d6cf5ff2 Initial version of subworker starting
def tag_is_rust_worker(tag):
    return REPO.is_ancestor("0d40c8432f2a3830fe36b8f7b498a9e9d6cf5ff2", tag)


def scheduler_binary_path(workdir, tag):
    return os.path.join(workdir, f"rsds-scheduler-{tag}")


def worker_binary_path(workdir, tag):
    return os.path.join(workdir, f"rsds-worker-{tag}")


def worker_python_path(workdir, tag):
    return os.path.join(workdir, "python", tag)


def copy_binary(name, target):
    binary = os.path.join(BUILD_DIR, name)
    assert os.path.isfile(binary)
    shutil.copyfile(binary, target)
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


def build_tag(workdir, tag):
    REPO.git.checkout(tag)

    src_python = os.path.join(ROOT_DIR, "python")
    if tag_is_rust_worker(tag):
        assert os.path.isdir(src_python)
        python_dir = worker_python_path(workdir, tag)
        if os.path.isdir(python_dir):
            shutil.rmtree(python_dir)
        shutil.copytree(src_python, python_dir)

    targets = [scheduler_binary_path(workdir, tag)]
    if tag_is_rust_worker(tag):
        targets.append(worker_binary_path(workdir, tag))

    if not all(os.path.isfile(t) for t in targets):
        env = os.environ.copy()
        env["RUSTFLAGS"] = "-C target-cpu=native"
        subprocess.run(["cargo", "build", "--release"], env=env, check=True)

        copy_binary("rsds-scheduler", scheduler_binary_path(workdir, tag))
        if tag_is_rust_worker(tag):
            copy_binary("rsds-worker", worker_binary_path(workdir, tag))


def build_rsds_cluster(workdir, tag, nodes, processes, debug):
    scheduler = {
        "name": f"rsds-{tag}",
        "binary": scheduler_binary_path(workdir, tag),
        "args": ["--scheduler", "workstealing"]
    }
    if debug:
        scheduler["env"] = {
            "RUST_LOG": "debug"
        }
    if tag_is_rust_worker(tag):
        workers = {
            "name": ["rworker"],
            "binary": [worker_binary_path(workdir, tag)],
            "rust": [True],
            "nodes": nodes,
            "processes": processes,
            "subworker": [worker_python_path(workdir, tag)]
        }
        if debug:
            workers["env"] = [{
                "RUST_LOG": "debug"
            }]
    else:
        workers = {
            "name": ["dworker"],
            "nodes": nodes,
            "threads": [1],
            "processes": processes,
            "args": [["--memory-limit", "10GB"]]
        }
    return {
        "$product": {
            "scheduler": [scheduler],
            "workers": {
                "$product": workers
            }
        }
    }


def build_dask_cluster(nodes, processes):
    scheduler = {
        "name": "dask",
        "binary": "dask-scheduler",
        "args": ["--no-dashboard"]
    }
    workers = {
        "name": ["dworker"],
        "nodes": nodes,
        "threads": [1],
        "processes": processes,
        "args": [["--memory-limit", "10GB"]]
    }
    return {
        "$product": {
            "scheduler": [scheduler],
            "workers": {
                "$product": workers
            }
        }
    }

def create_json(workdir, tags, nodes, processes, debug=False):
    clusters = [build_rsds_cluster(workdir, tag, nodes, processes, debug) for tag in tags]
    clusters.append(build_dask_cluster(nodes, processes))
    return {
        "repeat": 2,
        "reference": "dask",
        "configurations": {
            "$product": {
                "cluster": {"$ref": "clusters"},
                "usecase": {"$ref": "usecases"}
            }
        },
        "clusters": {"$+": clusters},
        "usecases": {"$+": [
            # {"$ref": "xarray"},
            {"$ref": "pandas_join"},
            {"$ref": "pandas_groupby"},
            {"$ref": "bag"},
            {"$ref": "tree"},
            {"$ref": "numpy"},
            {"$ref": "merge"},
            # {"$ref": "merge_slow"},
            # {"$ref": "wordbatch_vectorizer"},
            # {"$ref": "wordbatch_wordbag"}
        ]},
        "pandas_join": {
            "$product": {
                "function": ["pandas_join"],
                "args": [[1, "1s", "1T"], [1, "1s", "1H"], [1, "2s", "1H"]]
            }
        },
        "pandas_groupby": {
            "$product": {
                "function": ["pandas_groupby"],
                "args": [[30, "1s", "1H"], [90, "1s", "8H"]]#, [360, "1s", "1H"], [360, "1s", "8H"]]
            }
        },
        "xarray": {
            "$product": {
                "function": ["xarray"],
                "args": [5, 25]
            }
        },
        "bag": {
            "$product": {
                "function": ["bag"],
                "args": [[10000, 10], [10000, 20], [10000, 40]]
            }
        },
        "tree": {
            "$product": {
                "function": ["tree"],
                "args": [15]
            }
        },
        "numpy": {
            "$product": {
                "function": ["numpy"],
                "args": [[20000, 10], [20000, 20], [20000, 40]]
            }
        },
        "merge": {
            "$product": {
                "function": ["merge"],
                "args": [10000, 25000]
            }
        },
        "merge_slow": {
            "$product": {
                "function": ["merge_slow"],
                "args": [[5000, 0.1]]
            }
        },
        "wordbatch_vectorizer": {
            "$product": {
                "function": ["wordbatch_vectorizer"],
                "args": [
                    ["/scratch/work/project/dd-19-39/wordbatch/wordbatch.csv", 500000, 20],
                    ["/scratch/work/project/dd-19-39/wordbatch/wordbatch.csv", 500000, 200]
                ],
                "needs_client": [True]
            }
        },
        "wordbatch_wordbag": {
            "$product": {
                "function": ["wordbatch_wordbag"],
                "args": [
                    ["/scratch/work/project/dd-19-39/wordbatch/wordbatch.csv", 500000, 20],
                    ["/scratch/work/project/dd-19-39/wordbatch/wordbatch.csv", 500000, 200]
                ],
                "needs_client": [True]
            }
        }
    }


@click.command()
@click.argument("workdir")
@click.argument("tags")
@click.option("--processes", type=str, default="1")
@click.option("--local", type=bool, default=False)
@click.option("--bootstrap", type=str, default=None)
@click.option("--debug", type=bool, default=False)
def benchmark(workdir, tags, processes, local, bootstrap, debug):
    tags = tags.split(",")
    workdir = os.path.abspath(workdir)
    jobs_dir = os.path.join(workdir, "jobs")
    nodes = ["local"] if local else [1, 7]
    processes = [int(p) for p in processes.split(",")]

    with git_stash():
        for tag in tags:
            print(f"Building {tag}")
            build_tag(workdir, tag)

    data = create_json(workdir, tags, nodes, processes, debug=debug)
    print(json.dumps(data, indent=2))

    json_path = f"/tmp/perftest-{int(time.time())}"
    with open(json_path, "w") as f:
        json.dump(data, f)

    if not local:
        submit(input=json_path,
               name="jobs",
               nodes=8,
               queue="qexp",
               walltime=3600,
               workdir=workdir,
               project="",
               profile="",
               bootstrap=bootstrap,
               workon=DEFAULT_VENV,
               watch=False,
               postprocess=True)
    else:
        execute_benchmark(input=json_path,
                          output_dir=jobs_dir,
                          profile="",
                          timeout=3600,
                          bootstrap=bootstrap)
        generate_all(jobs_dir)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)

    benchmark()
