import functools
import json
import os
import time
import traceback
from multiprocessing.pool import ThreadPool

import dask
from distributed import Client
from usecases import bench_numpy, bench_pandas_groupby, bench_pandas_join, bench_bag, bench_merge, bench_merge_slow, \
    bench_tree, bench_xarray, bench_wordbatch_vectorizer, bench_wordbatch_wordbag, bench_merge_variable, bench_shuffle, \
    bench_len

USECASES = {
    "xarray": bench_xarray,
    "tree": bench_tree,
    "bag": bench_bag,
    "numpy": bench_numpy,
    "merge": bench_merge,
    "merge_slow": bench_merge_slow,
    "merge_variable": bench_merge_variable,
    "pandas_groupby": bench_pandas_groupby,
    "pandas_join": bench_pandas_join,
    "wordbatch_vectorizer": bench_wordbatch_vectorizer,
    "wordbatch_wordbag": bench_wordbatch_wordbag,
    "shuffle": bench_shuffle,
    "len": bench_len
}
CLIENT_TIMEOUT = 60
TIMEOUT_POOL = None


def with_timeout(fn, timeout):
    global TIMEOUT_POOL

    if TIMEOUT_POOL is None:
        TIMEOUT_POOL = ThreadPool(1)

    fut = TIMEOUT_POOL.apply_async(fn)
    return fut.get(timeout=timeout)


def compute(function):
    graph = function()
    start = time.time()
    result = dask.compute(graph)
    duration = time.time() - start
    return result, duration


def exit_and_print(obj):
    print(json.dumps(obj))
    exit()


if __name__ == "__main__":
    with open(os.environ["ARGUMENTS_PATH"]) as f:
        arguments = json.load(f)

    scheduler_address = arguments["scheduler_address"]
    required_workers = arguments["required_workers"]
    function_name = arguments["function_name"]
    needs_client = arguments["needs_client"]
    cluster_info = arguments["cluster_info"]
    args = arguments["args"]

    function = functools.partial(USECASES[function_name], *args)

    with Client(scheduler_address, timeout=CLIENT_TIMEOUT) as client:
        try:
            with_timeout(lambda: client.wait_for_workers(required_workers), CLIENT_TIMEOUT)
        except TimeoutError:
            exit_and_print(
                {"error": f"Cluster {cluster_info} did not start in {CLIENT_TIMEOUT}s: {traceback.format_exc()}"})
        assert len(client.scheduler_info()["workers"]) == required_workers

        if needs_client:
            res = function(client)
        else:
            res = compute(function)
        result, duration = res
        exit_and_print({
            "result": result,
            "duration": duration
        })
