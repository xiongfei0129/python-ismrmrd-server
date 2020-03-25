
import constants
from connection import Connection

import socket
import logging
import multiprocessing

import simplefft
import invertcontrast

class Server:
    """
    Something something docstring.
    """

    def __init__(self, address, port):

        logging.info("Initializing server. [%s %d]", address, port)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((address, port))

    def serve(self):
        logging.info("Serving... ")
        self.socket.listen(0)

        while True:
            sock, (remote_addr, remote_port) = self.socket.accept()

            logging.info("Accepting connection from: %s (%d)", remote_addr, remote_port)

            process = multiprocessing.Process(target=self.handle, args=[sock])
            process.daemon = True
            process.start()

            logging.info("Spawned process %d to handle connection.", process.pid)

    def handle(self, sock):

        try:
            connection = Connection(sock)

            # First two messages are config and parameter scripts. Read these.
            config = next(connection)
            parameters = next(connection)

            # Decide what program to use based on config
            # As a shortcut, we accept the file name as text too.
            if (config == "simplefft"):
                logging.info("Starting simplefft processing based on config")
                simplefft.process(connection, config, metadata)
            elif (config == "invertcontrast"):
                logging.info("Starting invertcontrast processing based on config")
                invertcontrast.process(connection, config, metadata)
            else:
                logging.info("Unknown config '%s'.  Falling back to 'invertcontrast'", config)
                invertcontrast.process(connection, config, metadata)

        except Exception as e:
            logging.exception(e)

        finally:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            logging.info("Socket closed")

