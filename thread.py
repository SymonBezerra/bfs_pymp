from collections import deque
from io import BytesIO
import socket

from graph import Edge
from message import Message

class Thread:
    def __init__(self, ip, port1, port2):
        self.receiving_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiving_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.receiving_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)

        # Set Type of Service (TOS) for QoS
        self.receiving_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0x10)  # IPTOS_LOWDELAY

        self.receiving_socket.setblocking(False)

        self.receiving_socket.bind((ip, port1))

        self.confirmation_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.confirmation_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.confirmation_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)

        # Set Type of Service (TOS) for QoS
        self.confirmation_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0x10)  # IPTOS_LOWDELAY

        self.confirmation_socket.bind((ip, port2))

        # adjacency list, source → destinies kept in the clients
        self.edges = dict()
        # cache buffer to keep track of visited nodess
        self.cache = dict()
        self.bytes_buffer = BytesIO()

    def recv(self):
        try:
            data, addr = self.receiving_socket.recvfrom(65507)
            msg = Message(data[:20].strip(), data[20:].strip())
            return msg, addr
        except BlockingIOError:
            return None, None

    def exec(self, msg, addr):
        # data, addr = self.socket.recvfrom(65507)
        # msg = Message(data[:20].strip(), data[20:].strip())
        header = msg.header
        if header == b'ADD_NODE':
            node = msg.body
            self.edges[node] = []  # Use label (bytes) as key
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)

        elif header == b'ADD_EDGE':
            n1, n2, weight = msg.body.split()
            self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        elif header == b'ADD_EDGES':
            edges = msg.body.split(b'|')
            for edge in edges:
                if edge == b'': continue
                n1, n2, weight = edge.split(b',')
                self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        elif header == b'ADD_NODES':
            nodes = msg.body.split(b'|')
            for node in nodes:
                if node == b'': continue
                self.edges[node] = []
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        elif header == b'INIT_BFS' or header == b'INIT_DFS':
            self.cache['visited'] = set()
            self.cache['nodes_added'] = set()
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        elif header == b'BFS' or header == b'DFS':
            node = msg.body
            if node in self.cache['visited']:
                self.confirmation_socket.sendto(Message(b'VISITED', b'').build(), addr)
            else:
                # self.cache['visited'].add(node)
                self.confirmation_socket.sendto(Message(b'NOT_VISITED', b'').build(), addr)
        elif header == b'GET_EDGES' or header == b'GET_EDGES_BFS' or header == b'GET_EDGES_DFS':
            node = msg.body

            batch_count = 0

            if header == b'GET_EDGES_BFS': edges = self.bfs(node)
            else: edges = self.edges[node]
            self.bytes_buffer.write(b'VISITED'.ljust(20))
            visited_batch_count = 0
            for visited in self.cache['visited']:
                visited_batch_count += 1
                self.bytes_buffer.write(visited)
                self.bytes_buffer.write(b',')
                if visited_batch_count == 500:
                    visited_batch_count = 0
                    self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'VISITED'.ljust(20))

            self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()
            self.bytes_buffer.write(b'EDGE'.ljust(20))
            while True:
                answer, _ = self.recv()
                if answer: break

            for edge in edges:
                self.bytes_buffer.write(edge.src)
                self.bytes_buffer.write(b',')
                self.bytes_buffer.write(edge.dest)
                self.bytes_buffer.write(b',')
                self.bytes_buffer.write(str(edge.weight).encode())
                self.bytes_buffer.write(b'|')
                batch_count += 1
                if batch_count == 500:
                    batch_count = 0
                    self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                    while True:
                        answer, _ = self.recv()
                        if answer: break
                    if answer[:20].strip() != b'OK': break
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'EDGE'.ljust(20))

            self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()
            while True:
                answer, _ = self.recv()
                if answer: break
            self.confirmation_socket.sendto(Message(b'DONE', b'').build(), addr)

        elif header == b'VISIT_NODE':
            node = msg.body
            self.cache['visited'].add(node)
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        # return data

    def send(self, header, body, ip, port):
        self.confirmation_socket.sendto(Message(header, body).build(), (ip, int(port)))
        answer, address = self.socket.recvfrom(65507)
        return Message(answer.decode()[:20].strip(), answer.decode()[20:].strip())
    
    def bfs(self, node):
        if node in self.cache['visited']:
            return []
        nodes = deque([node])
        edges = list()
        while nodes:
            current = nodes.popleft()
            for edge in self.edges[current]:
                dest = edge.dest
                if dest not in self.cache['nodes_added']:
                    if self.edges.get(dest) is not None:
                        nodes.append(dest)
                    self.cache['nodes_added'].add(dest)
                    edges.append(edge)
            self.cache['visited'].add(current)
        return edges