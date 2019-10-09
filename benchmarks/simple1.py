from dask.distributed import Client
from dask import delayed
from dask.distributed import wait

client = Client("tcp://localhost:7070")


@delayed
def do_something(x):
    return x * 10


xs = [do_something(x) for x in range(20000)]
result = merge(xs)
print(result.compute())
