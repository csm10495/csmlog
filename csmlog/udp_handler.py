"""
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2019) - Charles Machalow
"""

import logging
import logging.handlers
import socket

MAX_BYTES_PER_SEND = 1024


class UdpHandler(logging.StreamHandler):
    """handler to send live logs as raw text to a UDP socket"""

    stream = None

    def __init__(self, ip="127.0.0.1", port=5123):
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logging.StreamHandler.__init__(self)

    def __repr__(self):
        return "<UdpHandler %s:%s>" % (self.ip, self.port)

    def emit(self, record):
        msg = (self.format(record) + "\n").encode()
        msgLen = len(msg)
        offset = 0

        # make sure we send all
        while offset < msgLen:
            offset += self.socket.sendto(
                msg[offset : offset + MAX_BYTES_PER_SEND], (self.ip, self.port)
            )
