import asyncio
import concurrent
import pickle
import sys
from concurrent.futures.thread import ThreadPoolExecutor

import cloudpickle
import logging

from .conn import SocketWrapper
from .serialize import serialize, serialize_by_pickle, deserialize_by_pickle, deserialize
from .utils import substitude_keys

logger = logging.getLogger(__name__)


class Subworker:
    def __init__(self, subworker_id: int, socket: SocketWrapper):
        self.subworker_id = subworker_id
        self.socket = socket
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.loop = asyncio.get_event_loop()

    async def run(self):
        await self.socket.send_message({
            "subworker_id": self.subworker_id,
        })
        while True:
            message = await self.socket.receive_message()
            op = message["op"]
            if op == "ComputeTask":
                await self.handle_compute_task(message)
            else:
                raise Exception("Unknown command: {}".format(op))

    async def get_uploads(self, uploads):
        result = {}
        count = 0
        for upload in uploads:
            key = upload["key"]
            logger.info("Uploading %s from worker", key)
            data = await self.socket.read_raw_message()
            count += 1
            try:
                result[key] = deserialize(data, upload["serializer"])
            except Exception as e:
                # Read (and throw away) remaining data
                for _ in range(count, len(uploads)):
                    await self.socket.read_raw_message()
                raise e
        return result

    async def handle_compute_task(self, message):
        key = message.get("key")
        logger.info("Starting task %s", key)
        uploads = message.get("uploads")
        if uploads:
            logger.info("Need %s uploads", len(uploads))
            deps = await self.get_uploads(uploads)
        else:
            deps = {}

        async def inner():
            state, result = await self.loop.run_in_executor(self.executor, run_task, message, deps)
            if state == "ok":
                logger.info("Task %s successfully finished", key)
                serializer, data = serialize(result)
                await self.socket.send_message({
                    "op": "TaskFinished",
                    "serializer": serializer,
                    "key": key,
                    "result": data
                })
            else:
                exception, traceback = result
                logger.error("Task %s failed: %s", key, result)
                await self.socket.send_message({
                    "op": "TaskFailed",
                    "key": key,
                    "exception": serialize_by_pickle(exception),
                    "traceback": serialize_by_pickle(traceback),
                })
        self.loop.create_task(inner())


def run_task(message, deps):
    try:
        function = deserialize_by_pickle(message["function"])
        args = deserialize_by_pickle(message["args"])
        args = [substitude_keys(a, deps) for a in args]
        kwargs = message.get("kwargs", None)
        if kwargs:
            kwargs = deserialize_by_pickle(kwargs)
            result = function(*args, **kwargs)
        else:
            result = function(*args)
        return "ok", result
    except Exception as e:
        _, _, traceback = sys.exc_info()
        return "error", (e, traceback)
