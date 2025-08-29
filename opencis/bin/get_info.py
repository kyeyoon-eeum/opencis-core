"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import click

from opencis.bin import socketio_client


@click.group(name="get-info")
def get_info_group():
    """Command group for component info"""


@get_info_group.command(name="port")
def get_port():
    # Disabled in synchronous mode
    return


@get_info_group.command(name="vcs")
def get_vcs():
    # Disabled in synchronous mode
    return


@get_info_group.command(name="device")
def get_device():
    # Disabled in synchronous mode
    return
