from dask.distributed import Client
import dask.array as da
from utils import timer

client = Client("tcp://localhost:8786")

with timer("Init"):
    x = da.random.random((25000, 25000), chunks=(1000, 1000))

with timer("Compute"):
    y = x + x.T
    z = y[::2, 5000:].mean(axis=1)
    z.compute()
