import socket
import os

import pickle, cloudpickle

from .conn import SocketWrapper


def unpack(data):
    try:
        return pickle.loads(data)
    except:
        return cloudpickle.loads(data)


class Subworker:

    def __init__(self, subworker_id, connection: SocketWrapper):
        self.subworker_id = subworker_id
        self.connection = connection

    def run_task(self, message):
        function = unpack(message["function"])
        args = unpack(message["args"])

        kwargs = message.get("kwargs", None)
        if kwargs:
            kwargs = unpack(kwargs)
            result = function(*args, **kwargs)
        else:
            result = function(*args)

        print(message, result)
        import sys
        sys.stdout.flush()

    def run(self):
        self.connection.send_message({
            "subworker_id": self.subworker_id,
        })
        while True:
            message = self.connection.receive_message()
            assert message["op"] == "ComputeTask"
            self.run_task(message)