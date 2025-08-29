"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from yaml import dump

# Standard Python client setup for Socket.IO
# SocketIO client disabled in sync mode
sio = None


# Notifications disabled; define no-op handlers without decorators


def handle_port_updated():
    print("[Notification]")
    print("port:updated")


def handle_vcs_updated():
    print("[Notification]")
    print("port:updated")


def handle_device_updated():
    print("[Notification]")
    print("device:updated")


# Remove asyncio usage entirely


class CustomSemaphore:
    def __init__(self, value: int = 0, custom_value=None):
        self.custom_value = custom_value

    def set_custom_value(self, value):
        self.custom_value = value


def send(event, param=None):
    raise RuntimeError("socketio client disabled in synchronous mode")


def print_result(data):
    print(dump(data, sort_keys=False, default_flow_style=False))


# Connect event handler


def connect():
    pass


# Disconnect event handler


def disconnect():
    pass


def get_port():
    raise RuntimeError("socketio client disabled in synchronous mode")


def get_vcs():
    raise RuntimeError("socketio client disabled in synchronous mode")


def get_device():
    raise RuntimeError("socketio client disabled in synchronous mode")


# Bind & unbind


def bind(vcs: int, vppb: int, physical_port: int, ld_id: int = 0):
    raise RuntimeError("socketio client disabled in synchronous mode")


def unbind(vcs: int, vppb: int):
    raise RuntimeError("socketio client disabled in synchronous mode")


def get_ld_info(port_index: int):
    raise RuntimeError("socketio client disabled in synchronous mode")


def get_ld_allocation(port_index: int, start_ld_id: int, ld_allocation_list_limit: int):
    raise RuntimeError("socketio client disabled in synchronous mode")


def set_ld_allocation(
    port_index: int, number_of_lds: int, start_ld_id: int, ld_allocation_list: int
):
    raise RuntimeError("socketio client disabled in synchronous mode")


def freeze(vcs: int, vppb: int):
    raise RuntimeError("socketio client disabled in synchronous mode")


def unfreeze(vcs: int, vppb: int):
    raise RuntimeError("socketio client disabled in synchronous mode")


# Main synchronous stubs


def start_client():
    raise RuntimeError("socketio client disabled in synchronous mode")


def stop_client():
    raise RuntimeError("socketio client disabled in synchronous mode")


# Run the client
if __name__ == "__main__":
    pass
