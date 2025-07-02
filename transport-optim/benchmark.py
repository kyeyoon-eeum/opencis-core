import argparse
import threading
import time
from transport import Server, Client

FRAME_SIZE = 82  # bytes per packet


# ── server side ─────────────────────────────────────────────────────────
def _server_loop(iters: int, port: int, stop_event: threading.Event):
    try:
        srv = Server(b"0.0.0.0", port, kind=0)      # blocks until accept()
        frame = b"x" * FRAME_SIZE
        for _ in range(iters):
            srv._tp.send(frame)                     # one fire-and-forget write
        srv.stop()
    finally:
        stop_event.set()


# ── client side ─────────────────────────────────────────────────────────
def _client_loop(iters: int, port: int, stop_event: threading.Event):
    target_bytes = iters * FRAME_SIZE
    received = 0
    t0 = time.perf_counter()
    try:
        cli = Client(b"127.0.0.1", port)            # returns once connect() ok
        while received < target_bytes:
            chunk = cli._tp.recv(82)              # bulk read
            if chunk:
                received += len(chunk)
            else:
                time.sleep(0.00002)                 # light back-off
        dt = time.perf_counter() - t0
        mb = received / (1024 * 1024)
        print(
            f"[Benchmark] {iters:,} packets • "
            f"{mb / dt:8.2f} MB/s "
            f"(processed {mb:.3f} MB in {dt * 1e3:.2f} ms)"
        )
    finally:
        cli.stop()
        stop_event.set()


# ── harness ─────────────────────────────────────────────────────────────
def benchmark(iters: int, port: int = 9000):
    stop_event = threading.Event()
    srv = threading.Thread(target=_server_loop, args=(iters, port, stop_event), daemon=True)
    cli = threading.Thread(target=_client_loop, args=(iters, port, stop_event), daemon=True)
    srv.start()
    cli.start()
    cli.join()
    srv.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Throughput benchmark for transport.pyx")
    parser.add_argument(
        "iters", nargs="?", type=int, default=100_000, help="number of packets to send"
    )
    parser.add_argument("--port", type=int, default=9000, help="TCP port to use")
    args = parser.parse_args()
    benchmark(args.iters, args.port)
