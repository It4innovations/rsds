import socket
import os

from .conn import SocketWrapper


class Subworker:

    def __init__(self, subworker_id, connection: SocketWrapper):
        self.subworker_id = subworker_id
        self.connection = connection

    def run(self):
        self.connection.send_message({
            "subworker_id": self.subworker_id,
        })