import argparse
import threading
import time
import sys
from transport import Server, Client

def server_thread(port: int, iters: int, stop_event: threading.Event):
    # print(f"[Server] Starting server thread on port {port} for {iters:,} iterations")
    def handle(pkt: bytes):
        # print(f"[Server] Received packet: {len(pkt)} bytes")
        pass
    try:
        srv = Server(b"0.0.0.0", port, kind=0)
        # print(f"[Server] Server initialized")
        # Wait for client to connect
        while True:
            try:
                fd = srv._tp.fd
                # print(f"[Server] Transport fd: {fd}")
                if fd != -1:
                    break
            except AttributeError:
                # print("[Server] Error: _tp has no fd attribute")
                stop_event.set()
                return
            # print("[Server] Waiting for client connection...")
            time.sleep(0.001)
        # print(f"[Server] Client connected (fd={fd})")
        data = b"x" * 82  # 82 bytes of data
        sent = 0
        for i in range(iters):
            if stop_event.is_set():
                # print(f"[Server] Stop event set, exiting at iteration {i}")
                break
            sent_bytes = srv._tp.send(data)
            sent += 1
            # print(f"[Server] Sent packet {sent}/{iters} ({sent_bytes} bytes)")
            if sent_bytes != 82:
                # print(f"[Server] Warning: Sent {sent_bytes} bytes, expected 82")
                pass
            time.sleep(0.0001)  # Small delay to avoid overwhelming client
        # print(f"[Server] Finished sending {sent} packets")
        srv.stop()
        # print("[Server] Server stopped")
    except Exception as e:
        # print(f"[Server] Error: {e}")
        stop_event.set()

def client_thread(port: int, iters: int, stop_event: threading.Event):
    # print(f"[Client] Starting client thread, connecting to 127.0.0.1:{port}")
    try:
        cli = Client(b"127.0.0.1", port)
        # print("[Client] Client connected")
        try:
            fd = cli._tp.fd
            # print(f"[Client] Transport fd: {fd}")
        except AttributeError:
            # print("[Client] Error: _tp has no fd attribute")
            stop_event.set()
            return
        received = 0
        t0 = time.perf_counter()
        timeout = time.time() + 30.0  # 30-second timeout
        while received < iters and not stop_event.is_set() and time.time() < timeout:
            data = cli._tp.recv(82)
            if data:
                # print(f"[Client] Received packet {received+1}/{iters} ({len(data)} bytes)")
                received += 1
                time.sleep(0.0001)  # Small delay to flush socket buffer
            else:
                # print("[Client] No data received, retrying...")
                time.sleep(0.001)
        dt = time.perf_counter() - t0
        # print(f"[Client] Finished receiving {received}/{iters} packets")
        cli.stop()
        # print("[Client] Client stopped")
        stop_event.set()
        processed_mb = received * 82 / 1024 / 1024  # Use actual received packets
        if dt > 0:  # Avoid division by zero
            print(
                f"[Benchmark] {received:,}/{iters:,} packets • {processed_mb/dt:6.2f} MB/s "
                f"(processed {processed_mb:.3f} MB in {dt:.3f} s)"
            )
        else:
            print(f"[Benchmark] Error: Zero time recorded, received {received} packets")
    except Exception as e:
        # print(f"[Client] Error: {e}")
        stop_event.set()

def benchmark(iters: int):
    # print(f"[Main] Starting benchmark with {iters:,} iterations")
    port = 9000
    stop_event = threading.Event()
    srv_thread = threading.Thread(target=server_thread, args=(port, iters, stop_event), daemon=True)
    cli_thread = threading.Thread(target=client_thread, args=(port, iters, stop_event), daemon=True)
    # print("[Main] Starting server thread")
    srv_thread.start()
    time.sleep(0.1)  # Give server time to set up
    # print("[Main] Starting client thread")
    cli_thread.start()
    # print("[Main] Waiting for client thread to complete")
    cli_thread.join()
    # print("[Main] Client thread finished")
    srv_thread.join()
    # print("[Main] Server thread finished")
    # print("[Main] Benchmark completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark server-client throughput")
    parser.add_argument("iters", type=int, default=100_000, nargs="?", help="Number of iterations (packets to send)")
    args = parser.parse_args()
    benchmark(args.iters)