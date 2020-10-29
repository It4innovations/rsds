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
        result = []
        for upload in uploads:
            key = upload["key"]
            logger.info("Uploading %s from worker", key)
            data = await self.socket.read_raw_message()
            result.append((key, data, upload["serializer"]))
        return result

    async def handle_compute_task(self, message):
        key = message.get("key")
        logger.info("Starting task %s", key)
        uploads = message.get("uploads")
        if uploads:
            logger.info("Need %s uploads", len(uploads))
            upload_data = await self.get_uploads(uploads)
        else:
            upload_data = ()

        async def inner():
            state, result = await self.loop.run_in_executor(self.executor, run_task, message, upload_data)
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

def _is_dask_composed_task(obj):
    return isinstance(obj, tuple) and obj and callable(obj[0])


def _run_dask_composed_task(obj):
    if isinstance(obj, tuple) and obj and callable(obj[0]):
        function = obj[0]
        return function(*(_run_dask_composed_task(o) for o in obj[1:]))
    if isinstance(obj, list):
        return [_run_dask_composed_task(o) for o in obj]
    return obj


def run_task(message, uploads):
    try:
        logger.debug("Deserializing function")
        function = deserialize_by_pickle(message["function"])
        args = message["args"]
        if args is not None:
            logger.debug("Deserializing args")
            args = deserialize_by_pickle(args)
        else:
            args = ()
        kwargs = message.get("kwargs", None)
        if kwargs:
            logger.debug("Deserializing kwargs")
            kwargs = deserialize_by_pickle(kwargs)
        else:
            kwargs = {}
        deps = {}
        for key, data, serializer in uploads:
            logger.debug("Deserializing upload %s", key)
            deps[key] = deserialize(data, serializer)

        if callable(function):
            logger.debug("Starting normal function")
            args = [substitude_keys(a, deps) for a in args]
            result = function(*args, **kwargs)
        else:
            function = substitude_keys(function, deps)
            result = _run_dask_composed_task(function)
        return "ok", result
    except Exception as e:
        _, _, traceback = sys.exc_info()
        return "error", (e, traceback)
