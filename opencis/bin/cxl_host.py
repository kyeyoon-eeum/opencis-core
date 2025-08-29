"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import click

from opencis.util.logger import logger
from opencis.cxl.environment import parse_cxl_environment
from opencis.cxl.component.cxl_component import PORT_TYPE
from opencis.apps.memory_pooling import run_host


@click.group(name="host")
def host_group():
    """Command group for managing CXL Host"""


def start(port, ig, iw):
    logger.info(f"Starting CXL Host on Port{port}")
    # Run synchronously
    from opencis.apps.memory_pooling import run_host
    run_host(port_index=port, irq_port=8500, ig=ig, iw=iw)


def start_host_manager():
    logger.info("Starting CXL HostManager")
    host_manager = HostManager()
    host_manager.start_wait_ready()
    host_manager.join()


def run_host_group(ports, ig, iw):
    from opencis.apps.memory_pooling import run_host
    base_irq_port = 8500
    try:
        threads = []
        for i, idx in enumerate(ports):
            t = __import__("threading").Thread(
                target=run_host,
                args=(),
                kwargs={"port_index": idx, "irq_port": base_irq_port + i, "ig": ig, "iw": iw},
                daemon=True,
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
    except Exception as e:
        logger.error("Error while running CXL Host Group", exc_info=e)


def start_group(config_file: str, ig: int = 0, iw: int = 0):
    logger.info(f"Starting CXL Host Group - Config: {config_file}")
    try:
        environment = parse_cxl_environment(config_file)
    except Exception as e:
        logger.error(f"Failed to parse environment configuration: {e}")
        return

    ports = []
    for idx, port_config in enumerate(environment.switch_config.port_configs):
        if port_config.type == PORT_TYPE.USP:
            ports.append(idx)

    try:
        run_host_group(ports, ig, iw)
    except Exception as e:
        logger.error("Error while running CXL Host Group", exc_info=e)
