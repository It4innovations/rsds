import sys

from dask import delayed
from dask.distributed import Client
from utils import timer

client = Client("tcp://localhost:8786")


@delayed
def do_something(x):
    return x * 10


@delayed
def merge(*args):
    return sum(args)


with timer("Merge"):
    xs = [do_something(x) for x in range(int(sys.argv[1]))]
    result = merge(*xs)
    print(result.compute())
