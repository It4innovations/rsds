import json
import os
import shutil
import stat
import subprocess
import time

import click
from git import Repo
from benchmark import submit

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
BUILD_DIR = os.path.join(ROOT_DIR, "target/release")
WORKDIR = os.path.join(CURRENT_DIR, "perftest")

REPO = Repo(ROOT_DIR)


def binary_name(tag):
    return f"rsds-{tag}"


def binary_path(tag):
    return os.path.join(BUILD_DIR, binary_name(tag))


def build_tag(tag):
    REPO.git.checkout(tag)
    subprocess.run(["cargo", "build", "--release"], env={
        "RUSTFLAGS": "-C target-cpu=native"
    }, check=True)
    binary = os.path.join(BUILD_DIR, "rsds-scheduler")
    target = os.path.join(BUILD_DIR, binary_name(tag))
    shutil.copyfile(binary, target)
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


def create_json(tags):
    schedulers = {
        f"rsds-{tag}": {
            "name": f"rsds-{tag}",
            "binary": binary_path(tag),
            "args": ["--scheduler", "workstealing"]
        } for tag in tags
    }

    base = {
        "repeat": 5,
        "configurations": {
            "$product": {
                "cluster": {"$ref": "clusters"},
                "usecase": {"$ref": "usecases"}
            }
        },
        "clusters": {
            "$product": {
                "scheduler": [
                    {"$ref": f"rsds-{tag}" for tag in tags},
                ],
                "workers": {
                    "$+": [
                        {"$product": {
                            "nodes": [1, 7],
                            "threads": [1],
                            "processes": [24],
                            "args": [["--memory-limit", "10GB"]],
                            "name": ["sw"]
                        }},
                        {"$product": {
                            "nodes": [1, 7],
                            "threads": [1],
                            "processes": [24],
                            "binary": ["$BUILD/target/release/rsds-worker"],
                            "spawn-all": [True],
                            "name": ["zw"]
                        }}]
                }
            }
        },
        "usecases": {"$+": [
            {"$ref": "xarray"},
            {"$ref": "pandas_join"},
            {"$ref": "pandas_groupby"},
            {"$ref": "bag"},
            {"$ref": "tree"},
            {"$ref": "numpy"},
            {"$ref": "merge"},
            {"$ref": "merge_slow"},
            {"$ref": "wordbatch_vectorizer"},
            {"$ref": "wordbatch_wordbag"}
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
                "args": [[90, "1s", "1H"], [90, "1s", "8H"], [360, "1s", "1H"], [360, "1s", "8H"]]
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
                "args": [10000, 15000, 25000, 50000]
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
    base.update(schedulers)
    return base


@click.command()
@click.argument("name")
@click.argument("tags", nargs=-1)
@click.option("--bootstrap", type=bool, default=False)
def benchmark(name, tags, bootstrap):
    for tag in tags:
        print(f"Building {tag} into {binary_name(tag)}")
        build_tag(tag)

    if bootstrap:
        bootstrap = os.path.join(WORKDIR, name, "result.json")
    else:
        bootstrap = None

    data = create_json(tags)
    print(data)

    json_path = f"/tmp/perftest-{int(time.time())}"
    with open(json_path, "w") as f:
        json.dump(data, f)

    submit(input=json_path,
           name=name,
           nodes=8,
           queue="qexp",
           walltime=3600,
           workdir=WORKDIR,
           project=None,
           profile="",
           bootstrap=bootstrap,
           workon=None,
           watch=False,
           postprocess=True)


if __name__ == "__main__":
    benchmark()
