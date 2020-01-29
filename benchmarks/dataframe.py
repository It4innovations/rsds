import dask
from dask.distributed import Client

from utils import timer

client = Client("tcp://localhost:8786")


with timer("Init"):
    df = dask.datasets.timeseries(start="2020-01-30", end="2020-01-31")
    print(len(df))

with timer("Groupby"):
    df.groupby("name")["x"].mean().compute()

with timer("Filter"):
    df[(df["x"] > 0) | (df["y"] < 0)].compute()
