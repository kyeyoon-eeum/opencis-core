#!/usr/bin/env python3
"""
Ultra-fast benchmark using optimized 82-byte methods for 200+ MB/s
"""

import argparse
import multiprocessing as mp
import time
import os
from fast_transport import FastTransport

FRAME_SIZE = 82  # Fixed 82-byte frames


def cleanup_files(name: str):
    """Clean up shared memory files"""
    for suffix in ["_tx", "_rx"]:
        try:
            os.unlink(f"/tmp/fastbuf_{name}{suffix}.bin")
        except FileNotFoundError:
            pass


def ultra_producer_process(name: str, iters: int):
    """Ultra-optimized producer using 82-byte method"""
    try:
        producer = FastTransport.create_producer(name)
        
        # Pre-create the 82-byte frame
        frame = b"x" * FRAME_SIZE
        
        # Reduced delay since consumer timing is now more accurate
        time.sleep(0.1)  # 100ms to ensure consumer is ready
        
        print(f"[Producer] Ultra-fast sending {iters:,} x 82-byte packets")
        
        t0 = time.perf_counter()
        
        # Tight loop using optimized 82-byte method
        for _ in range(iters):
            producer.send_82(frame)
        
        dt = time.perf_counter() - t0
        
        total_mb = (iters * FRAME_SIZE) / (1024 * 1024)
        throughput = total_mb / dt
        
        print(f"[Producer] Sent {iters:,} packets • {throughput:8.2f} MB/s • {total_mb:.2f} MB in {dt*1000:.2f} ms")
        
    except Exception as e:
        print(f"[Producer] Error: {e}")
        import traceback
        traceback.print_exc()


def ultra_consumer_process(name: str, iters: int):
    """Ultra-optimized consumer using 82-byte method"""
    try:
        consumer = FastTransport.create_consumer(name)
        
        print(f"[Consumer] Ultra-fast receiving {iters:,} x 82-byte packets")
        
        received_packets = 0
        t0 = None  # Start timing when first packet arrives
        
        # Tight loop using optimized 82-byte method
        while received_packets < iters:
            data = consumer.recv_82()
            if data:
                if t0 is None:
                    t0 = time.perf_counter()  # Start timing on first packet
                received_packets += 1
        
        dt = time.perf_counter() - t0
        
        total_mb = (received_packets * FRAME_SIZE) / (1024 * 1024)
        throughput = total_mb / dt
        
        print(f"[Consumer] Received {received_packets:,} packets • {throughput:8.2f} MB/s • {total_mb:.2f} MB in {dt*1000:.2f} ms")
        
    except Exception as e:
        print(f"[Consumer] Error: {e}")
        import traceback
        traceback.print_exc()


def ultra_benchmark(iters: int, name: str = "ultrabench"):
    """Run ultra-fast benchmark with 64MB buffers and optimized methods"""
    
    cleanup_files(name)
    
    print(f"=== Ultra-Fast Transport Benchmark ===")
    print(f"Target: 200+ MB/s")
    print(f"Packets: {iters:,}")
    print(f"Frame size: {FRAME_SIZE} bytes (fixed, no headers)")
    print(f"Total data: {(iters * FRAME_SIZE) / (1024*1024):.2f} MB")
    print(f"Buffer size: 64MB each (128MB total)")
    print(f"Optimizations: Fixed-size messages, cache-aligned buffers, optimized spinning")
    print()
    
    # Start consumer first to create buffers
    consumer_proc = mp.Process(target=ultra_consumer_process, args=(name, iters))
    producer_proc = mp.Process(target=ultra_producer_process, args=(name, iters))
    
    consumer_proc.start()
    time.sleep(0.2)  # Let consumer initialize
    producer_proc.start()
    
    try:
        producer_proc.join(timeout=60)
        consumer_proc.join(timeout=60)
        
        if producer_proc.is_alive():
            print("[Warning] Producer timed out")
            producer_proc.terminate()
            producer_proc.join()
            
        if consumer_proc.is_alive():
            print("[Warning] Consumer timed out")
            consumer_proc.terminate()
            consumer_proc.join()
            
    except KeyboardInterrupt:
        print("\nBenchmark interrupted")
        producer_proc.terminate()
        consumer_proc.terminate()
        producer_proc.join()
        consumer_proc.join()
    
    cleanup_files(name)


def ultra_demo_application(num_requests: int):
    """Ultra-fast demo application using optimized 82-byte methods"""
    
    def ultra_server_process(name: str):
        """Ultra-fast server using 82-byte methods"""
        try:
            server = FastTransport.create_consumer(name)
            response_frame = b"R" * FRAME_SIZE  # Response frame
            
            requests_handled = 0
            start_time = time.time()
            
            print(f"[Server] Ultra-fast server starting...")
            
            while requests_handled < num_requests:
                request = server.recv_82()
                if request:
                    requests_handled += 1
                    server.send_82(response_frame)
                    
                    if requests_handled % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = requests_handled / elapsed
                        mb_per_sec = (requests_handled * FRAME_SIZE * 2) / (1024 * 1024) / elapsed
                        print(f"[Server] {requests_handled:,} • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")
            
            elapsed = time.time() - start_time
            rate = requests_handled / elapsed
            mb_per_sec = (requests_handled * FRAME_SIZE * 2) / (1024 * 1024) / elapsed
            print(f"[Server] Final: {requests_handled:,} • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")
            
        except Exception as e:
            print(f"[Server] Error: {e}")
    
    def ultra_client_process(name: str):
        """Ultra-fast client using 82-byte methods"""
        try:
            client = FastTransport.create_producer(name)
            request_frame = b"Q" * FRAME_SIZE  # Request frame
            
            print(f"[Client] Ultra-fast client for {num_requests:,} requests...")
            
            responses_received = 0
            start_time = time.time()
            
            # Send all requests using optimized method
            send_start = time.time()
            for _ in range(num_requests):
                client.send_82(request_frame)
            send_time = time.time() - send_start
            print(f"[Client] Sent all requests in {send_time*1000:.1f} ms")
            
            # Receive all responses using optimized method
            while responses_received < num_requests:
                response = client.recv_82()
                if response:
                    responses_received += 1
                    
                    if responses_received % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = responses_received / elapsed
                        mb_per_sec = (responses_received * FRAME_SIZE * 2) / (1024 * 1024) / elapsed
                        print(f"[Client] {responses_received:,} • {rate:,.0f} resp/s • {mb_per_sec:.1f} MB/s")
            
            elapsed = time.time() - start_time
            rate = responses_received / elapsed
            total_mb = (num_requests * FRAME_SIZE * 2) / (1024 * 1024)
            mb_per_sec = total_mb / elapsed
            print(f"[Client] Final: {num_requests:,} round-trips • {rate:,.0f} req/s • {mb_per_sec:.1f} MB/s")
            
        except Exception as e:
            print(f"[Client] Error: {e}")
    
    name = "ultrademo"
    cleanup_files(name)
    
    print(f"\n=== Ultra-Fast Demo Application ===")
    print(f"Requests: {num_requests:,}")
    print(f"64MB buffers with 82-byte fixed-size optimization")
    print()
    
    server_proc = mp.Process(target=ultra_server_process, args=(name,))
    client_proc = mp.Process(target=ultra_client_process, args=(name,))
    
    server_proc.start()
    time.sleep(0.3)  # Extra time for large buffer initialization
    client_proc.start()
    
    try:
        client_proc.join(timeout=120)
        server_proc.join(timeout=120)
        
        if client_proc.is_alive():
            print("[Warning] Client timed out")
            client_proc.terminate()
            client_proc.join()
            
        if server_proc.is_alive():
            print("[Warning] Server timed out")
            server_proc.terminate()
            server_proc.join()
            
    except KeyboardInterrupt:
        print("\nDemo interrupted")
        client_proc.terminate()
        server_proc.terminate()
        client_proc.join()
        server_proc.join()
    
    cleanup_files(name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ultra-fast transport benchmark for 200+ MB/s")
    parser.add_argument("iters", nargs="?", type=int, default=5_000_000, 
                       help="Number of packets to send (default: 5,000,000)")
    parser.add_argument("--demo", action="store_true", 
                       help="Run demo application instead of benchmark")
    
    args = parser.parse_args()
    
    if args.demo:
        ultra_demo_application(args.iters)
    else:
        ultra_benchmark(args.iters) 