import time
from transport import Client

cli = Client(b"127.0.0.1", 9000)

for i in range(5):
    cli.send(f"hello {i}".encode())
    time.sleep(0.1)

cli.send(b"quit")
cli.stop()

