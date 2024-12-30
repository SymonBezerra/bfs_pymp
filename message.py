from io import BytesIO

class Message:
    def __init__(self, header: bytes, body: bytes):
        self.header = header
        self.body = body

    def build(self):
        with BytesIO() as buffer:
            buffer.write(self.header)
            buffer.write(''.ljust(20).encode())
            buffer.write(self.body)
            return buffer.getvalue()