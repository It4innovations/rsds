import contextlib
import time


@contextlib.contextmanager
def timer(name):
    start = time.time()
    yield
    print("{}: {}".format(name, time.time() - start))
