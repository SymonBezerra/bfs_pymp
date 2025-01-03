from collections import deque
from io import BytesIO
import socket

from graph import Edge
from message import Message

class Thread:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        # Set high priority for network traffic
        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)

        # Set Type of Service (TOS) for QoS
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0x10)  # IPTOS_LOWDELAY

        self.socket.bind((ip, port))

        # adjacency list, source → destinies kept in the clients
        self.edges = dict()
        # cache buffer to keep track of visited nodess
        self.cache = dict()
        self.bytes_buffer = BytesIO()

    def recv(self):
        data, addr = self.socket.recvfrom(65507)
        msg = Message(data[:20].strip(), data[20:].strip())
        if msg.header == b'ADD_NODE':
            node = msg.body
            self.edges[node] = []  # Use label (bytes) as key
            self.socket.sendto(Message(b'OK', b'').build(), addr)

        elif msg.header == b'ADD_EDGE':
            n1, n2, weight = msg.body.split()
            self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        elif msg.header == b'ADD_EDGES':
            edges = msg.body.split(b'|')
            for edge in edges:
                if edge == b'': continue
                n1, n2, weight = edge.split(b',')
                self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        elif msg.header == b'INIT_BFS' or msg.header == b'INIT_DFS':
            self.cache['visited'] = set()
            self.cache['nodes_added'] = set()
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        elif msg.header == b'BFS' or msg.header == b'DFS':
            node = msg.body
            if node in self.cache['visited']:
                self.socket.sendto(Message(b'VISITED', b'').build(), addr)
            else:
                # self.cache['visited'].add(node)
                self.socket.sendto(Message(b'NOT_VISITED', b'').build(), addr)
        elif msg.header == b'GET_EDGES' or msg.header == b'GET_EDGES_BFS' or msg.header == b'GET_EDGES_DFS':
            node = msg.body

            batch_count = 0

            if msg.header == b'GET_EDGES_BFS': edges = self.bfs(node)
            else: edges = self.edges[node]
            self.bytes_buffer.write(b'VISITED'.ljust(20))
            for visited in self.cache['visited']:
                self.bytes_buffer.write(visited)
                self.bytes_buffer.write(b',')
            self.socket.sendto(self.bytes_buffer.getvalue(), addr)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()
            self.bytes_buffer.write(b'EDGE'.ljust(20))
            self.socket.recvfrom(65507) # await confirmation
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
                    self.socket.sendto(self.bytes_buffer.getvalue(), addr)
                    answer, _ = self.socket.recvfrom(65507)
                    if answer[:20].strip() != b'OK': break
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'EDGE'.ljust(20))

            self.socket.sendto(self.bytes_buffer.getvalue(), addr)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()
            self.socket.recvfrom(1024) # await confirm65507n
            self.socket.sendto(Message(b'DONE', b'').build(), addr)

        elif msg.header == b'VISIT_NODE':
            node = msg.body
            self.cache['visited'].add(node)
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        return data

    def send(self, header, body, ip, port):
        self.socket.sendto(Message(header, body).build(), (ip, int(port)))
        answer, address = self.socket.recvfrom(65507)
        return Message(answer.decode()[:20].strip(), answer.decode()[20:].strip())
    
    def bfs(self, node):
        nodes = deque([node])
        edges = []
        if node in self.cache['visited']:
            print('visited')
            return edges
        while nodes:
            current = nodes.popleft()
            for edge in self.edges[current]:
                if edge.dest not in self.cache['nodes_added']:
                    if self.edges.get(edge.dest) is not None:
                        nodes.append(edge.dest)
                    self.cache['nodes_added'].add(edge.dest)
                    edges.append(edge)
            self.cache['visited'].add(current)
        return edges