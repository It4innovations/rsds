import asyncio
import os
import struct
from asyncio import StreamReader, StreamWriter

import msgpack


async def connect_to_unix_socket(socket_path):
    # Protection against long filenames, socket names are limited
    backup = os.getcwd()
    try:
        os.chdir(os.path.dirname(socket_path))
        reader, writer = await asyncio.open_unix_connection(socket_path)
    finally:
        os.chdir(backup)

    return SocketWrapper(reader, writer)


class SocketWrapper:
    header = struct.Struct("<I")
    header_size = 4
    read_buffer_size = 32 * 1024

    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self._buffer = bytes()

    async def send_message(self, message):
        return await write_message(message, self.writer)

    async def receive_message(self):
        return await read_message(self.reader)


async def write_message(message, writer: StreamWriter):
    message = msgpack.dumps(message)
    data = SocketWrapper.header.pack(len(message)) + message
    writer.write(data)
    return await writer.drain()


async def read_message(reader: StreamReader):
    header = await reader.readexactly(SocketWrapper.header_size)
    msg_size = SocketWrapper.header.unpack(header)[0]
    return msgpack.loads(await reader.readexactly(msg_size))
