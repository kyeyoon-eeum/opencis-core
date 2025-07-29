#!/usr/bin/env python3
"""
Demo application showing high-performance inter-process transport usage

This demonstrates:
1. True inter-process communication at 200+ MB/s
2. Future-proof design for XDP hardware acceleration
3. Simple API that can be used in real applications
"""

import argparse
import multiprocessing as mp
import time
import signal
import sys
from fast_transport import FastTransport

FRAME_SIZE = 82  # Fixed 82-byte frames for optimization


def cleanup_handler(signum, frame):
    """Clean up shared memory on interrupt"""
    import os
    for name in ["demo_tx", "demo_rx"]:
        try:
            os.unlink(f"/tmp/fastbuf_{name}.bin")
        except:
            pass
    sys.exit(0)


def server_process(name: str, mode: str = "shm"):
    """
    Server process that receives requests and sends responses
    
    In the future, mode="xdp" will use AF_XDP sockets for hardware acceleration
    """
    signal.signal(signal.SIGINT, cleanup_handler)
    
    print(f"[Server] Starting in {mode} mode...")
    
    # Create transport (consumer for requests, producer for responses)
    server = FastTransport.create_consumer(name)
    
    # Pre-create the 82-byte response frame for maximum performance
    response_frame = b"Response-" + b"x" * (FRAME_SIZE - 9)  # Pad to exactly 82 bytes
    
    requests_handled = 0
    start_time = time.time()
    
    try:
        while True:
            # Receive request using optimized 82-byte method
            request = server.recv_82()
            if request:
                requests_handled += 1
                
                # Send response using optimized 82-byte method
                server.send_82(response_frame)
                
                # Print stats every 100K requests for ultra-fast performance tracking
                if requests_handled % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = requests_handled / elapsed
                    mb_per_sec = (requests_handled * FRAME_SIZE * 2) / (1024 * 1024) / elapsed  # *2 for req+resp
                    print(f"[Server] {requests_handled:,} • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")
                
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        rate = requests_handled / elapsed if elapsed > 0 else 0
        mb_per_sec = (requests_handled * FRAME_SIZE * 2) / (1024 * 1024) / elapsed if elapsed > 0 else 0
        print(f"\n[Server] Shutting down. Handled {requests_handled:,} requests • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")


def client_process(name: str, num_requests: int, mode: str = "shm"):
    """
    Client process that sends requests and receives responses
    Uses ultra-fast 82-byte optimized methods with 64MB buffers
    """
    signal.signal(signal.SIGINT, cleanup_handler)
    
    print(f"[Client] Starting in {mode} mode, sending {num_requests:,} requests...")
    
    # Create transport (producer for requests, consumer for responses)
    client = FastTransport.create_producer(name)
    
    # Pre-create the 82-byte request frame for maximum performance
    request_frame = b"Request--" + b"x" * (FRAME_SIZE - 9)  # Pad to exactly 82 bytes
    
    responses_received = 0
    start_time = time.time()
    
    try:
        # Send all requests using ultra-fast 82-byte method
        send_start = time.time()
        for i in range(num_requests):
            client.send_82(request_frame)
        send_time = time.time() - send_start
        print(f"[Client] Sent all requests in {send_time*1000:.1f} ms")
        
        # Receive all responses using ultra-fast 82-byte method
        while responses_received < num_requests:
            response = client.recv_82()
            if response:
                responses_received += 1
                
                # Print progress every 100K responses for ultra-fast tracking
                if responses_received % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = responses_received / elapsed
                    mb_per_sec = (responses_received * FRAME_SIZE * 2) / (1024 * 1024) / elapsed  # *2 for req+resp
                    print(f"[Client] {responses_received:,} • {rate:,.0f} resp/s • {mb_per_sec:.1f} MB/s")
        
        total_time = time.time() - start_time
        rate = num_requests / total_time
        total_mb = (num_requests * FRAME_SIZE * 2) / (1024 * 1024)  # *2 for req+resp
        mb_per_sec = total_mb / total_time
        print(f"[Client] Final: {num_requests:,} round-trips • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")
        
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        rate = responses_received / elapsed if elapsed > 0 else 0
        mb_per_sec = (responses_received * FRAME_SIZE * 2) / (1024 * 1024) / elapsed if elapsed > 0 else 0
        print(f"\n[Client] Interrupted. Received {responses_received:,} responses • {rate:,.0f} resp/s • {mb_per_sec:.1f} MB/s")


def run_demo(num_requests: int, transport_mode: str = "shm"):
    """
    Run the demo with server and client processes
    
    Args:
        num_requests: Number of requests to send
        transport_mode: "shm" for shared memory, "xdp" for future XDP acceleration
    """
    if transport_mode == "xdp":
        print("XDP mode will be available in future versions with:")
        print("- AF_XDP socket support")
        print("- Zero-copy packet processing")
        print("- Hardware-accelerated networking")
        print("- Sub-microsecond latency")
        print("Using shared memory mode for now...\n")
        transport_mode = "shm"
    
    name = "demo"
    
    # Clean up any existing shared memory
    import os
    for suffix in ["_tx", "_rx"]:
        try:
            os.unlink(f"/tmp/fastbuf_{name}{suffix}.bin")
        except:
            pass
    
    print(f"=== Ultra-Fast Transport Demo ===")
    print(f"Transport: {transport_mode}")
    print(f"Requests: {num_requests:,}")
    print(f"Frame size: {FRAME_SIZE} bytes (fixed, ultra-optimized)")
    print(f"Total data: {(num_requests * FRAME_SIZE * 2) / (1024 * 1024):.1f} MB (req + resp)")
    print(f"Buffer size: 64MB each (128MB total)")
    print(f"Optimizations: Fixed-size messages, cache-aligned buffers, optimized spinning")
    print()
    
    # Start server process
    server_proc = mp.Process(target=server_process, args=(name, transport_mode))
    server_proc.start()
    
    # Give server time to initialize 64MB buffers
    time.sleep(0.3)
    
    # Start client process
    client_proc = mp.Process(target=client_process, args=(name, num_requests, transport_mode))
    client_proc.start()
    
    try:
        # Wait for client to complete with longer timeout for large requests
        client_proc.join(timeout=120)
        
        if client_proc.is_alive():
            print("[Warning] Client timed out")
            client_proc.terminate()
            client_proc.join()
        
        # Give server a moment to finish processing
        time.sleep(0.1)
        
        # Terminate server
        server_proc.terminate()
        server_proc.join()
        
    except KeyboardInterrupt:
        print("\nDemo interrupted")
        client_proc.terminate()
        server_proc.terminate()
        client_proc.join()
        server_proc.join()
    
    # Clean up
    for suffix in ["_tx", "_rx"]:
        try:
            os.unlink(f"/tmp/fastbuf_{name}{suffix}.bin")
        except:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ultra-fast transport demo application")
    parser.add_argument("requests", nargs="?", type=int, default=500_000,
                       help="Number of requests to send (default: 500,000)")
    parser.add_argument("--mode", choices=["shm", "xdp"], default="shm",
                       help="Transport mode: shm (shared memory) or xdp (future)")
    
    args = parser.parse_args()
    
    run_demo(args.requests, args.mode) 