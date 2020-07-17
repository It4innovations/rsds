import asyncio
import concurrent
import pickle
import sys
from concurrent.futures.thread import ThreadPoolExecutor

import cloudpickle

from .conn import SocketWrapper


def unpack(data):
    try:
        return pickle.loads(data)
    except:
        return cloudpickle.loads(data)


class Subworker:
    def __init__(self, subworker_id: int, socket: SocketWrapper):
        self.subworker_id = subworker_id
        self.socket = socket
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.loop = asyncio.get_event_loop()
        self.handlers = {
            "ComputeTask": self.handle_compute_task
        }

    async def run(self):
        await self.socket.send_message({
            "subworker_id": self.subworker_id,
        })
        while True:
            message = await self.socket.receive_message()
            handler = self.handlers[message["op"]]
            await handler(message)

    async def handle_compute_task(self, message):
        async def inner():
            key = message.get("key")

            try:
                result = await self.loop.run_in_executor(self.executor, run_task, message)
                await self.socket.send_message({
                    "op": "TaskFinished",
                    "key": key,
                    "result": dumps(result)
                })
            except Exception as e:
                await self.socket.send_message({
                    "op": "TaskErrored",
                    "key": key,
                    "error": dumps(e)
                })

        self.loop.create_task(inner())


def dumps(data):
    return pickle.dumps(data)


def run_task(message):
    function = unpack(message["function"])
    args = unpack(message["args"])

    kwargs = message.get("kwargs", None)
    if kwargs:
        kwargs = unpack(kwargs)
        result = function(*args, **kwargs)
    else:
        result = function(*args)

    return result
