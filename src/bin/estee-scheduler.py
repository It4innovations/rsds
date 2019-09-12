import asyncio
import json
import logging
import struct

from estee.schedulers import AllOnOneScheduler
from estee.schedulers.tasks import TaskState


async def command_parser(reader):
    while True:
        data = await reader.read(4)
        if not data:
            break
        assert len(data) == 4
        length = struct.unpack(">I", data)[0]
        assert length > 0

        buffer = bytearray()
        remaining = length
        while remaining > 0:
            data = await reader.read(min(4096, remaining))
            if not data:
                break
            buffer += data
            remaining -= len(data)
        message = json.loads(buffer)
        logging.info("Receive: {}".format(message))
        yield message


async def send_command(writer, cmd):
    logging.info("Send: {}".format(cmd))
    serialized = json.dumps(cmd).encode("utf-8")
    length = len(serialized)
    len_buffer = struct.pack(">I", length)
    writer.write(len_buffer + serialized)
    await writer.drain()


async def scheduler_server(reader, writer, scheduler):
    await send_command(writer, {"Register": {
        "protocol_version": 0,
        "scheduler_name": "estee_scheduler",
        "scheduler_version": "0.0",
        "reassigning": False
    }})

    async for command in command_parser(reader):
        await handle_command(writer, scheduler, command)

    writer.close()


TASK_STATE_MAP = {
    "Waiting": TaskState.Waiting,
    "Running": TaskState.Assigned,
    "Finished": TaskState.Finished
}


async def handle_command(writer, scheduler, command):
    cmd_type = list(command.keys())[0]
    payload = command[cmd_type]
    if cmd_type == "Update":
        msg = {"type": "update"}
        if "task_updates" in payload:
            msg["tasks_update"] = [{
                "id": t["id"],
                "state": TASK_STATE_MAP[t["state"]],
                "worker": t.get("worker"),
                "running": t["state"] == "Running"
            } for t in payload["task_updates"]]
        if "new_workers" in payload:
            msg["new_workers"] = [{
                "id": w["id"],
                "cpus": w["ncpus"]
            } for w in payload["new_workers"]]
        if "network_bandwidth" in payload:
            msg["network_bandwidth"] = payload["network_bandwidth"]
        if "new_tasks" in payload:
            # TODO
            msg["new_tasks"] = [{
                "id": t["id"],
                "inputs": t["inputs"],
                "outputs": [],
                "expected_duration": 1,
                "cpus": 1
            } for t in payload["new_tasks"]]
        schedule = scheduler.send_message(msg)
        response = []
        for assignment in schedule:
            response.append({
                "task": assignment["task"],
                "worker": assignment["worker"],
                "priority": 1
            })
        if response:
            await send_command(writer, {
                "TaskAssignments": response
            })
    else:
        assert False


def run_server(host, port, scheduler):
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(lambda r, w: scheduler_server(r, w, scheduler), host, port, loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    scheduler = AllOnOneScheduler()
    run_server("127.0.0.1", 8888, scheduler)
