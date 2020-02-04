from operator import add

import pytest
import time
from dask import delayed
from distributed.client import Client, Future


# Function for submitting in testing pipelines ----

def comp_fn1(x):
    return x * 10


def comp_fn2(x, y):
    return x - y


def comp_fn3(x):
    return [v + 1 for v in x]


def inc(x):
    return x + 1


@delayed
def delayed_fn1(x):
    return x * 10


@delayed
def delayed_fn2(x, y):
    return x + y


@delayed
def delayed_merge(*x):
    return x


class MyException(Exception):
    pass


@delayed
def error_fn(x):
    raise MyException("MyException")


# ---------------------------------------------

def test_submit_gather(rsds_env):
    url = rsds_env.start([1])
    client = Client(url)
    f1 = client.submit(comp_fn1, 10)
    f2 = client.submit(comp_fn1, 13)
    r1, r2 = client.gather([f1, f2])
    assert r1 == 100
    assert r2 == 130

    f1, f2 = client.map(comp_fn1, [20, 30])
    r1, r2 = client.gather([f1, f2])
    assert r1 == 200
    assert r2 == 300

    r1 = delayed_fn2(delayed_fn1(2), delayed_fn1(3)).compute()
    assert r1 == 50


def test_same_input(rsds_env):
    url = rsds_env.start([1])
    client = Client(url)

    f1 = client.submit(comp_fn1, 10)
    f2 = client.submit(comp_fn2, f1, f1)
    r2 = client.gather(f2)
    assert r2 == 0


def test_recompute_existing(rsds_env):
    url = rsds_env.start([1])
    client = Client(url)

    # assert delayed_fn1(10).compute() == 100
    # assert delayed_fn1(10).compute() == 100

    f1 = client.submit(comp_fn1, 10)
    f2 = client.submit(comp_fn1, 10)
    r1, r2 = client.gather([f1, f2])
    assert r1 == 100
    assert r2 == 100


def test_long_chain(rsds_env):
    url = rsds_env.start([1])
    client = Client(url)

    t = delayed_fn1(1)
    for _ in range(10):
        t = delayed_fn1(t)

    r = t.compute()
    assert r == 100_000_000_000


def test_more_clients(rsds_env):
    url = rsds_env.start([1])
    client1 = Client(url)
    client2 = Client(url)

    f1 = client1.submit(comp_fn1, 10)
    f2 = client2.submit(comp_fn1, 20)
    r2 = client2.gather(f2)
    r1 = client1.gather(f1)
    assert r1 == 100
    assert r2 == 200


def test_compute_error(rsds_env):
    url = rsds_env.start([1])
    _ = Client(url)
    with pytest.raises(MyException):
        delayed_fn1(error_fn(delayed_fn1(10))).compute()


def test_scatter(rsds_env):
    url = rsds_env.start([1])

    client = Client(url)
    client.wait_for_workers(1)

    metadata = client.scheduler_info()
    worker = list(metadata["workers"].keys())[0]
    futures = client.scatter(range(10), workers=[worker])
    fut = client.submit(comp_fn3, futures)
    assert client.gather(fut) == list(range(1, 11))


def test_gather_already_finished(rsds_env):
    url = rsds_env.start([1])

    client = Client(url)
    x = client.submit(inc, 10)
    assert not x.done()

    assert isinstance(x, Future)
    assert x.client is client

    result = client.gather(x)
    assert result == 11
    assert x.done()

    y = client.submit(inc, 20)
    z = client.submit(add, x, y)

    result = client.gather(z)
    assert result == 11 + 21


def test_stealing(rsds_env):
    client = Client(rsds_env.start([1, 1]))
    client.wait_for_workers(2)

    @delayed
    def delayed_sleep(x):
        time.sleep(x)
        return x

    @delayed
    def delayed_const(x):
        return x

    c1 = delayed_const(0.1)
    c2 = delayed_const(0.5)

    sleeps = []
    for i in range(4):
        sleeps.append(delayed_sleep(c1))
        sleeps.append(delayed_sleep(c2))

    rs = delayed_merge(*sleeps).compute()
    assert rs == (0.1, 0.5) * 4

"""
def test_resubmit(rsds_env):
    url = rsds_env.start([1])
    import numpy as np
    import dask.array as da
    client = Client(url)
    size = 1000
    for i in range(2):
        da.random.seed(0)
        x = da.random.random((size, size), chunks=(1000, 1000))
        y = x + x.T
        res = np.sum(y[::2, size / 2:].mean(axis=1).compute())
"""