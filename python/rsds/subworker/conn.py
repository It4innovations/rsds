import struct
import msgpack
import socket
import os


def connect_to_unix_socket(socket_path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Protection against long filenames, socket names are limited
    backup = os.getcwd()
    try:
        os.chdir(os.path.dirname(socket_path))
        sock.connect(os.path.basename(socket_path))
    finally:
        os.chdir(backup)

    return SocketWrapper(sock)


class SocketWrapper:

    header = struct.Struct("<I")
    header_size = 4
    read_buffer_size = 256 * 1024

    def __init__(self, sock):
        self.socket = sock
        self._buffer = bytes()

    def close(self):
        self.socket.close()

    def send_message(self, message):
        msg = msgpack.dumps(message)
        data = self.header.pack(len(msg)) + msg
        self.socket.sendall(data)

    def receive_message(self):
        header_size = self.header_size
        while True:
            size = len(self._buffer)
            if size >= header_size:
                msg_size = self.header.unpack(self._buffer[:header_size])[0] + header_size
                if size >= msg_size:
                    message = self._buffer[header_size:msg_size]
                    self._buffer = self._buffer[msg_size:]
                    return msgpack.loads(message)

            new_data = self.socket.recv(self.read_buffer_size)
            if not new_data:
                raise Exception("Connection to server lost")
            self._buffer += new_data
