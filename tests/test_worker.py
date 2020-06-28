from dask import delayed
from distributed.client import Client


@delayed
def delayed_fn1(x):
    return x * 10


@delayed
def delayed_merge(*x):
    return x


def test_worker_merge(rsds_env):
    url = rsds_env.start([1] * 4, rsds_worker=True)
    client = Client(url)
    # assert len(client.scheduler_info()['workers']) == 4

    f = delayed_fn1(1).compute()
    print(f)

    fs = [delayed_fn1(x) for x in range(22)]
    r = delayed_merge(*fs)
    print(r.compute())


def test_worker_x(rsds_env):
    url = rsds_env.start([1] * 1, rsds_worker=True)
    client = Client(url)

    f = delayed_fn1(1).compute()
    print(f)

    #f = delayed_fn1(1).compute()
    # print(f)
    #
    # fs = [delayed_fn1(x) for x in range(22)]
    # r = delayed_merge(*fs)
    # print(r.compute())
