import pytest
from dask import delayed
from distributed.client import Client


# Function for submitting in testing pipelines ----

def comp_fn1(x):
    return x * 10


def comp_fn2(x, y):
    return x - y


@delayed
def delayed_fn1(x):
    return x * 10


@delayed
def delayed_fn2(x, y):
    return x + y


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
