"""
Copyright (c) 2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import socket
import sys
from typing import Callable
import traceback
import threading

from opencis.util.component import RunnableComponent
from opencis.util.logger import logger


class ServerComponent(RunnableComponent):
    def __init__(
        self,
        handle_client: Callable,
        host: str = "0.0.0.0",
        port: int = 0,
        stop_callback: Callable = None,
        label: str = None,
        leave_opened: bool = False,
    ):
        if label is None:
            # Get caller function name
            # pylint: disable=protected-access
            label = sys._getframe(1).f_locals["self"].__class__.__name__
        super().__init__(label)

        if handle_client is None:
            raise ValueError("handle_client must be provided")

        self._host = host
        self._port = port
        self._handle_client = handle_client
        self._stop_callback = stop_callback
        self._descriptor = f"TCP server ({label})"
        self._leave_opened = leave_opened
        self._server_socket = None
        self._clients = set()
        self._stop_event = threading.Event()

    def get_port(self):
        return self._port

    def _create_server(self):
        if "SHM" in self._descriptor:
            return None
        logger.info(self._create_message(f"Starting {self._descriptor} server"))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self._host, self._port))
        s.listen()
        if self._port == 0:
            self._port = s.getsockname()[1]
        self._server_socket = s
        logger.info(
            self._create_message(f"{self._descriptor} listening on {self._host}:{self._port}")
        )
        return s

    def _run(self):
        try:
            logger.info(self._create_message(f"Creating {self._descriptor}"))
            server = self._create_server()
            if server is None:
                self._change_status_to_running()
                self._stop_event.wait()
                return

            def accept_loop():
                while not self._stop_event.is_set():
                    try:
                        client_sock, addr = server.accept()
                        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
                        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
                        logger.info(self._create_message(f"Found a new socket connection: {addr}"))

                        def client_thread(sock, address):
                            try:
                                # Wrap socket into minimal reader/writer like objects
                                class Reader:
                                    def __init__(self, s):
                                        self._s = s

                                    def read(self, n):
                                        return self._s.recv(n)

                                class Writer:
                                    def __init__(self, s):
                                        self._s = s

                                    def write(self, data: bytes):
                                        self._s.sendall(data)

                                    def close(self):
                                        try:
                                            self._s.shutdown(socket.SHUT_RDWR)
                                        except Exception:
                                            pass
                                        self._s.close()

                                reader = Reader(sock)
                                writer = Writer(sock)
                                self._handle_client(reader, writer)
                                if not self._leave_opened:
                                    writer.close()
                                    logger.info(
                                        self._create_message(f"Closed client connection: {address}")
                                    )
                            except Exception as e:
                                logger.error(
                                    self._create_message(
                                        f"Client error: {e}, {traceback.format_exc()}"
                                    )
                                )

                        t = threading.Thread(
                            target=client_thread, args=(client_sock, addr), daemon=True
                        )
                        t.start()
                    except OSError:
                        break

            t = threading.Thread(target=accept_loop, daemon=True)
            t.start()
            self._change_status_to_running()
            t.join()
        except Exception as e:
            logger.error(
                self._create_message(
                    f"{self._descriptor} error: {str(e)}, {traceback.format_exc()}"
                )
            )

    def _stop(self):
        logger.info(self._create_message(f"Stopping {self._descriptor} server"))
        self._stop_event.set()
        try:
            if self._server_socket is not None:
                try:
                    self._server_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                self._server_socket.close()
        except Exception:
            pass
