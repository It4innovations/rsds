import asyncio
import concurrent
import pickle
import msgpack
import sys
from concurrent.futures.thread import ThreadPoolExecutor

import cloudpickle
import logging

from .conn import SocketWrapper
from .serialize import (
    serialize,
    serialize_by_pickle,
    deserialize_by_pickle,
    deserialize,
)
from .utils import substitude_keys

logger = logging.getLogger(__name__)


class Subworker:
    def __init__(self, subworker_id: int, socket: SocketWrapper):
        self.subworker_id = subworker_id
        self.socket = socket
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.loop = asyncio.get_event_loop()
        self.worker_id = None

    async def run(self):
        await self.handshake()
        while True:
            message = await self.socket.receive_message()
            op = message["op"]
            if op == "ComputeTask":
                await self.handle_compute_task(message)
            else:
                raise Exception("Unknown command: {}".format(op))

    async def handshake(self):
        await self.socket.send_message(
            {
                "subworker_id": self.subworker_id,
            }
        )
        response = await self.socket.receive_message()
        logger.info("%s", response)
        self.worker_id = response["worker"]

    async def get_uploads(self, uploads):
        result = []
        for upload in uploads:
            data_id = upload["id"]
            logger.info("Uploading %s from worker", data_id)
            data = await self.socket.read_raw_message()
            result.append((data_id, data, upload["serializer"]))
        return result

    async def handle_compute_task(self, message):
        task_id = message["id"]
        logger.info("Starting task %s", task_id)
        uploads = message.get("uploads")
        if uploads:
            logger.info("Need %s uploads", len(uploads))
            upload_data = await self.get_uploads(uploads)
        else:
            upload_data = ()

        async def inner():
            state, result = await self.loop.run_in_executor(
                self.executor, run_task, message, upload_data
            )
            if state == "ok":
                logger.info("Task %s successfully finished", task_id)
                serializer, data = serialize(result)
                await self.socket.send_message(
                    {
                        "op": "TaskFinished",
                        "serializer": serializer,
                        "id": task_id,
                        "result": data,
                    }
                )
            else:
                exception, traceback = result
                logger.error("Task %s failed: %s", task_id, result)
                await self.socket.send_message(
                    {
                        "op": "TaskFailed",
                        "id": task_id,
                        "exception": serialize_by_pickle(exception),
                        "traceback": serialize_by_pickle(traceback),
                    }
                )

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
        logger.debug("Deserializing spec")
        spec = msgpack.loads(message["spec"])
        del message["spec"]  # Remove serialized spec
        logger.debug("Deserializing function")
        function = deserialize_by_pickle(spec["function"])
        args = spec.get("args")
        if args is not None:
            logger.debug("Deserializing args")
            args = deserialize_by_pickle(args)
        else:
            args = ()
        kwargs = spec.get("kwargs", None)
        if kwargs:
            logger.debug("Deserializing kwargs")
            kwargs = deserialize_by_pickle(kwargs)
        else:
            kwargs = {}

        id_key_map = spec.get("id_key_map")
        if id_key_map is not None:
            id_key_map = dict(id_key_map)
        deps = {}
        for data_id, data, serializer in uploads:
            logger.debug("Deserializing upload %s", data_id)
            deps[id_key_map[data_id]] = deserialize(data, serializer)
        del id_key_map

        if callable(function):
            logger.debug("Starting normal function")
            args = [substitude_keys(a, deps) for a in args]
            del deps
            result = function(*args, **kwargs)
        else:
            function = substitude_keys(function, deps)
            del deps
            result = _run_dask_composed_task(function)
        return "ok", result
    except Exception as e:
        _, _, traceback = sys.exc_info()
        return "error", (e, traceback)
