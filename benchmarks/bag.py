from dask.distributed import Client
from utils import timer
import dask
import json
import dask.bag as db
import os

client = Client("tcp://localhost:8786")


if not os.path.exists("data"):
    os.makedirs('data', exist_ok=True)              # Create data/ directory
    b = dask.datasets.make_people()                 # Make records of people
    b.map(json.dumps).to_textfiles('data/*.json')   # Encode as JSON, write to disk

b = db.read_text('data/*.json').map(json.loads)
b = db.concat([b for _ in range(400)])

with timer("Map/filt/count"):
    m = b.map(lambda record: record['occupation'])
    filt = m.filter(lambda record: len(record) > 6)
    res = m.map(lambda record: len(record))
    res = res.count().compute()
