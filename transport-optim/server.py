from transport import Server

def handle(pkt: bytes):
    print(" ⇒", pkt)
    if pkt == b"quit":
        print("bye!")
        exit(0)

srv = Server(b"0.0.0.0", 9000, kind=0)
srv.run(handle)

