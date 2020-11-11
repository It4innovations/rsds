import datetime
from operator import add

import dask
import dask.array as da
import pytest
from distributed import Client

with_all_schedulers = pytest.mark.parametrize("scheduler", [
    "workstealing",
    "blevel",
    "tlevel",
    "random"
])


@with_all_schedulers
def test_dataframe(rsds_env, scheduler):
    url = rsds_env.start([2], scheduler=scheduler)
    _ = Client(url)

    start = datetime.datetime(year=2020, month=1, day=1)
    end = start + datetime.timedelta(days=1)

    df = dask.datasets.timeseries(start=start, end=end, seed=0)
    m = df.groupby("name")["x"].mean().sum().compute()
    s = df[(df["x"] > 0) & (df["y"] < 0)]["x"].resample("2S").mean().sum().compute()
    assert (m, s) == (-0.04156885670869045, 9373.1532695118)


@with_all_schedulers
def test_bag(rsds_env, scheduler):
    url = rsds_env.start([2], scheduler=scheduler)
    _ = Client(url)

    _ = pytest.importorskip("mimesis")

    b = dask.datasets.make_people(seed=0, npartitions=10, records_per_partition=10)
    res = b.filter(lambda record: record['age'] > 30) \
        .map(lambda record: record['occupation']) \
        .frequencies(sort=True) \
        .topk(10, key=1) \
        .compute()
    assert sum(v[1] for v in res) == 13


@with_all_schedulers
def test_numpy(rsds_env, scheduler):
    url = rsds_env.start([2], scheduler=scheduler)
    _ = Client(url)

    np = pytest.importorskip("numpy")

    size = 500
    da.random.seed(0)
    x = da.random.random((size, size), chunks=(1000, 1000))
    y = x + x.T
    assert np.sum(y[::2, size / 2:].mean(axis=1).compute()) == 249.93142625694077


@with_all_schedulers
def test_tree(rsds_env, scheduler):
    url = rsds_env.start([2], scheduler=scheduler)
    _ = Client(url)

    exp = 5
    L = list(range(2 ** exp))
    while len(L) > 1:
        new_L = []
        for i in range(0, len(L), 2):
            lazy = add(L[i], L[i + 1])  # add neighbors
            new_L.append(lazy)
        L = new_L                       # swap old list for new

    assert dask.compute(L)[0][0] == 496


@with_all_schedulers
def test_xarray(rsds_env, scheduler):
    url = rsds_env.start([2], scheduler=scheduler)
    _ = Client(url)

    xr = pytest.importorskip("xarray")

    chunk_size = 20
    ds = xr.tutorial.open_dataset('air_temperature',
                                  chunks={'lat': chunk_size, 'lon': chunk_size, 'time': -1})
    da = ds['air']
    da2 = da.groupby('time.month').mean('time')
    da3 = da - da2
    x = da3.sum().load()

    assert x.values.flatten()[0] == 2239958.0


@pytest.mark.skip(reason="Actors not yet supported in RSDS worker")
def test_actor(rsds_env):
    url = rsds_env.start([2])
    client = Client(url)

    class Counter:
        n = 0

        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1
            return self.n

        def add(self, x):
            self.n += x
            return self.n

    future = client.submit(Counter, actor=True)
    counter = future.result()

    future = counter.increment()
    assert future.result() == 1

    future = counter.add(10)
    assert future.result() == 11
    assert counter.n == 11
