import os
import logging
from .subworker import Subworker
from .conn import connect_to_unix_socket


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

    Subworker(subworker_id, connect_to_unix_socket(socket_path)).run()



if __name__ == "__main__":
    main()