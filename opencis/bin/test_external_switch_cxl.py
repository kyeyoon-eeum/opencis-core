#!/usr/bin/env python3
"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import sys
import os

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from opencis.apps.cxl_switch import CxlSwitch


def main():
    """Main function to test external switch CXL functionality"""
    print("Testing external switch CXL functionality...")

    # Create and run the switch
    switch = CxlSwitch()
    try:
        switch.start_wait_ready()
        print("Switch started successfully")

        # Keep running for a bit to test
        import time

        time.sleep(5)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        switch.stop()
        print("Switch stopped")


if __name__ == "__main__":
    main()
