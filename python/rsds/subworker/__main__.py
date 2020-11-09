import uvloop

uvloop.install()  # noqa


import asyncio
import os
import logging
from .subworker import Subworker
from .conn import connect_to_unix_socket
from .globals import _set_global_subworker


def get_environ(name, type_=None):
    try:
        value = os.environ[name]
        if type_ is not None:
            return type_(value)
        return value
    except TypeError:
        raise Exception("Env variable {} has invalid value".format(name))
    except KeyError:
        raise Exception("Env variable {} is not set".format(name))


def main():
    logging.basicConfig(level=logging.INFO)

    subworker_id = get_environ("RSDS_SUBWORKER_ID", int)
    socket_path = get_environ("RSDS_SUBWORKER_SOCKET")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(subworker_id, socket_path))


async def run(subworker_id, socket_path):
    subworker = Subworker(subworker_id, await connect_to_unix_socket(socket_path))
    _set_global_subworker(subworker)
    await subworker.run()


if __name__ == "__main__":
    main()
