import asyncio
import json
import struct

import estee


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
        yield json.loads(buffer)


async def send_command(writer, cmd):
    serialized = json.dumps(cmd).encode("utf-8")
    length = len(serialized)
    len_buffer = struct.pack(">I", length)
    writer.write(len_buffer + serialized)
    await writer.drain()


async def scheduler_server(reader, writer):
    await send_command(writer, {"Register": {
        "protocol_version": 0,
        "scheduler_name": "estee_scheduler",
        "scheduler_version": "0.0",
        "reassigning": False
    }})

    async for command in command_parser(reader):
        await send_command(writer, {"hello": 5})

    writer.close()


loop = asyncio.get_event_loop()
coro = asyncio.start_server(scheduler_server, '127.0.0.1', 8888, loop=loop)
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
