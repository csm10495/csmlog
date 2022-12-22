"""
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2019) - Charles Machalow
"""

import socket
import sys
import threading

MAX_UDP_PACKET_SIZE = 65535


class UdpHandlerReceiver(object):
    """receiver to print live logs as raw text from a UDP socket"""

    def __init__(self, ip="127.0.0.1", port=5123, bufferMaxSize=MAX_UDP_PACKET_SIZE):
        self.ip = ip
        self.port = port
        self.bufferMaxSize = bufferMaxSize

        self.__stop = False
        self.__lock = threading.Lock()
        self.__buffer = ""

    def __repr__(self):
        return "<UdpHandlerReceiver %s:%s>" % (self.ip, self.port)

    def _appendToBuffer(self, data):
        with self.__lock:
            self.__buffer += data

            # chop buffer to latest x items
            self.__buffer = self.__buffer[-self.bufferMaxSize :]

    def getBuffer(self):
        with self.__lock:
            return self.__buffer

    def requestStop(self):
        with self.__lock:
            self.__stop = True

    def shouldStop(self):
        with self.__lock:
            return self.__stop

    def recieveForever(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.socket.bind((self.ip, self.port))
        try:
            while not self.shouldStop():
                # don't block to make this easier to ctrl-c
                try:
                    data = self.socket.recv(MAX_UDP_PACKET_SIZE).decode()
                except socket.error:
                    continue

                if len(data):
                    self._appendToBuffer(data)
                    sys.stdout.write(data)

        finally:
            self.socket.close()
            del self.socket


def main():
    u = UdpHandlerReceiver()
    u.recieveForever()


if __name__ == "__main__":
    main()
