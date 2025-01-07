from collections import defaultdict, deque
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
        self.edges = defaultdict(list)
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
        header = msg.header
        if header == b'RESTART':
            self.edges.clear()
            self.cache.clear()
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)

        elif header == b'ADD_NODE':
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
            self.cache['search_edges'] = defaultdict(deque)
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)
        elif header == b'BFS' or header == b'DFS':
            # self.cache['visited'] = set()
            nodes = [node for node in msg.body.split(b',') if node != b'' and node not in self.cache['visited']]
            if header == b'BFS':

                new_nodes, new_visited = self.bfs(nodes)
                if not new_nodes and not new_visited:
                    self.confirmation_socket.sendto(Message(b'DONE', b'').build(), addr)
                    return
                self.bytes_buffer.write(b'NEW_NODES'.ljust(20))
                batch_count = 0
                for nodes in new_nodes:
                    batch_count += 1
                    self.bytes_buffer.write(nodes[0])
                    self.bytes_buffer.write(b',')
                    self.bytes_buffer.write(nodes[1])
                    self.bytes_buffer.write(b'|')
                    if batch_count == 2000:
                        batch_count = 0
                        self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                        self.confirmation_socket.recvfrom(65507) # await confirmation
                        self.bytes_buffer.seek(0)
                        self.bytes_buffer.truncate()
                        self.bytes_buffer.write(b'NEW_NODES'.ljust(20))
                self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                self.bytes_buffer.seek(0)
                self.bytes_buffer.truncate()
                self.confirmation_socket.recvfrom(65507) # await confirmation

                self.bytes_buffer.write(b'VISITED'.ljust(20))
                batch_count = 0
                for visited in new_visited:
                    batch_count += 1
                    self.bytes_buffer.write(visited)
                    self.bytes_buffer.write(b',')
                    if batch_count == 2000:
                        batch_count = 0
                        self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                        self.confirmation_socket.recvfrom(65507)
                        self.bytes_buffer.seek(0)
                        self.bytes_buffer.truncate()
                        self.bytes_buffer.write(b'VISITED'.ljust(20))
                self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
                self.confirmation_socket.recvfrom(65507) # await confirmation
                self.bytes_buffer.seek(0)
                self.bytes_buffer.truncate()

                self.confirmation_socket.sendto(Message(b'DONE', b'').build(), addr)
        elif header == b'GET_EDGES' or header == b'GET_EDGES_BFS' or header == b'GET_EDGES_DFS':
            node = msg.body

            batch_count = 0
            if header == b'GET_EDGES': edges = self.bfs(node)
            elif header == b'GET_EDGES_BFS': edges = self.cache['search_edges'][node]

            self.bytes_buffer.write(b'EDGE'.ljust(20))
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
                    answer, _ = self.confirmation_socket.recvfrom(65507)
                    if answer[:20].strip() != b'OK': break
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'EDGE'.ljust(20))
            self.confirmation_socket.sendto(self.bytes_buffer.getvalue(), addr)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()
            self.confirmation_socket.recvfrom(1024) # await confirmation
            self.confirmation_socket.sendto(Message(b'DONE', b'').build(), addr)

        elif header == b'VISIT_NODE':
            node = msg.body
            self.cache['visited'].add(node)
            self.confirmation_socket.sendto(Message(b'OK', b'').build(), addr)

    def bfs(self, nodes):
        # if node in self.cache['visited']:
        #     return [], {}
        batch = deque(nodes)
        new_nodes = deque()
        new_visited = set()
        while batch:
            current = batch.popleft()
            if current in self.cache['visited']: 
                continue
            # Add this check
            elif current not in self.edges:
                # This node belongs to another thread
                # Still add it to visited to prevent cycles
                # self.cache['visited'].add(current)
                # Add it to new_nodes so master knows about it
                new_nodes.append((current, current))
                continue

            elif current in self.edges:
                for edge in self.edges[current]:
                    src = edge.src
                    dest = edge.dest
                    batch.append(src)
                    if dest not in self.cache['nodes_added']:
                        batch.append(dest)
                        self.cache['nodes_added'].add(dest)
                        self.cache['search_edges'][src].append(edge)
                        new_nodes.append((src, dest))
                self.cache['visited'].add(current)
                new_visited.add(current)
        return new_nodes, new_visited
