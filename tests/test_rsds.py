from distributed.client import Client
from dask import delayed


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
    assert r2 == 100


"""
FIX THIS
def test_recomute_existing(rsds_env):
    url = rsds_env.start([1])
    client = Client(url)

    assert delayed_fn1(10).compute() == 100
    assert delayed_fn2(10).compute() == 100
"""