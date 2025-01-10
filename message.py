class Message:
    def __init__(self, header: bytes, body: bytes):
        self.header = header
        self.body = body

    def build(self):
        return {'header': self.header, 'body': self.body}