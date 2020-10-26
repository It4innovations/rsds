from dask import delayed
from distributed.client import Client


@delayed
def delayed_fn1(x):
    return x * 2


@delayed
def delayed_merge(*x):
    return x


@delayed
def delayed_error():
    raise Exception("MyError")


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

    f = delayed_fn1(delayed_fn1(1)).compute()
    #f = delayed_fn1(1).compute()
    print(f)

    #f = delayed_fn1(1).compute()
    # print(f)
    #
    # fs = [delayed_fn1(x) for x in range(22)]
    # r = delayed_merge(*fs)
    # print(r.compute())


def test_worker_smallmerge(rsds_env):
    url = rsds_env.start([2, 2], rsds_worker=True)
    client = Client(url)

    @delayed
    def f1(x):
        import time
        time.sleep(0.5)
        return x + 1

    @delayed
    def f2(*x):
        return x

    f = f2(f2([f1(1), f1(2), f1(3), f1(4)])).compute()
    print(f)


def test_worker_error(rsds_env):
    url = rsds_env.start([1] * 1, rsds_worker=True)
    client = Client(url)
    f = delayed_error().compute()