import asyncio
import random
from asyncio import StreamReader

from cloudpickle import dumps, loads

import pytest

from python.rsds.subworker.conn import connect_to_unix_socket, SocketWrapper
from python.rsds.subworker.subworker import Subworker


async def unix_server(tmp_path):
    connect_fut = asyncio.Future()

    async def cb(reader, writer):
        connect_fut.set_result(SocketWrapper(reader, writer))

    path = str(tmp_path / "rsds-unix-socket")
    srv = await asyncio.start_unix_server(cb, path)
    return srv, path, connect_fut


async def create_subworker(tmp_path, id: int = 0):
    server, path, connect_fut = await unix_server(tmp_path)
    client_socket = await connect_to_unix_socket(path)
    control_socket = await connect_fut
    subworker = Subworker(id, client_socket)
    await init_subworker(subworker, control_socket)
    return subworker, control_socket


@pytest.mark.asyncio
async def test_send_receive(tmp_path):
    server, path, connect_fut = await unix_server(tmp_path)
    client_socket = await connect_to_unix_socket(path)
    control_socket = await connect_fut

    message = b"Hello world"
    await client_socket.send_message(message)
    assert await control_socket.receive_message() == message


async def init_subworker(subworker: Subworker, control_socket: SocketWrapper):
    asyncio.get_event_loop().create_task(subworker.run())
    response = await control_socket.receive_message()
    assert response == {"subworker_id": subworker.subworker_id}


@pytest.mark.asyncio
async def test_compute_success(tmp_path):
    subworker, control_socket = await create_subworker(tmp_path)

    key = "key"
    await control_socket.send_message({
        "op": "ComputeTask",
        "key": key,
        "function": dumps(lambda x: x + 1),
        "args": dumps([1])
    })

    response = await control_socket.receive_message()
    assert response["op"] == "TaskFinished"
    assert response["key"] == key
    assert loads(response["result"]) == 2


@pytest.mark.asyncio
async def test_compute_error(tmp_path):
    subworker, control_socket = await create_subworker(tmp_path)

    def func():
        raise Exception("foo")

    key = "key"
    await control_socket.send_message({
        "op": "ComputeTask",
        "key": key,
        "function": dumps(func),
        "args": dumps(())
    })

    response = await control_socket.receive_message()
    assert response["op"] == "TaskFailed"
    assert response["key"] == key
    assert str(loads(response["exception"])) == "foo"
    assert loads(response["traceback"]) is not None
