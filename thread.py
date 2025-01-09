from collections import defaultdict, deque
from io import BytesIO
# import socket

import msgpack
import zmq

from graph import Edge
from message import Message

class Thread:
    def __init__(self, ip, port, master_ip, master_port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)

        self.socket.bind(f'tcp://{ip}:{port}')
        self.socket.setsockopt(zmq.SNDHWM, 1000)  # Send high water mark
        self.socket.setsockopt(zmq.RCVHWM, 1000)  # Receive high water mark
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.socket.connect(f'tcp://{master_ip}:{master_port}')

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port + 1000}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect(f'tcp://{master_ip}:{master_port + 1000}')
        self.push_socket.setsockopt(zmq.SNDHWM, 1000)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # adjacency list, source → destinies kept in the clients
        self.edges = defaultdict(list)

        # cache buffer to keep track of visited nodess
        self.visited = set()
        self.nodes_added = set()
        self.search_edges = defaultdict(deque)
        self.bytes_buffer = BytesIO()

        self.__opt = {
            'node_batch': 500,
            'edge_batch': 500
        }

    def set_opt(self, opt, value):
        try:
            self.__opt[opt] = value
        except KeyError:
            raise ValueError(f"Invalid option: {opt}")

    def recv(self):
        data = self.socket.recv()
        msg = Message(data[:20].strip(), data[20:].strip())
        return msg

    def exec(self, msg_raw):
        msg = msgpack.unpackb(msg_raw, raw=False)
        header = msg['header']
        if header == b'RESTART':
            self.edges.clear()
            self.cache.clear()
            self.socket.send(msgpack.packb(Message(b'OK', b'').build()))

        elif header == b'ADD_NODE':
            node = msg.body
            self.edges[node] = []  # Use label (bytes) as key
            self.socket.send(msgpack.packb(Message(b'OK', b'').build()))

        elif header == b'ADD_EDGE':
            body = msg['body']
            n1, n2, weight = body.split(b',')
            self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_EDGES':
            edges = msg['body']
            for edge in edges:
                if edge == b'': continue
                n1, n2, weight = edge.split(b',')
                self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_NODES':
            nodes = msg.body.split(b'|')
            for node in nodes:
                if node == b'': continue
                self.edges[node] = []
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'INIT_BFS' or header == b'INIT_DFS':
            self.visited = set()
            self.nodes_added = set()
            self.search_edges = defaultdict(deque)
            self.socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'BFS' or header == b'DFS':
            # self.cache['visited'] = set()
            msg_body = msg['body']
            nodes = [node for node in msg_body if node != b'' and node not in self.visited]
            if header == b'BFS':
                new_nodes, new_visited = self.bfs(nodes)
                if not new_nodes and not new_visited:
                    self.push_socket.send(msgpack.packb(Message(b'DONE', b'').build()))
                    return
                
                msg = Message(b'RESULT', {'NEW_NODES': new_nodes, 'VISITED': new_visited}).build()

                # self.push_socket.send(msgpack.packb(Message(b'NEW_NODES', new_nodes).build()))
                # self.pull_socket.recv() # await confirmation

                # self.push_socket.send(msgpack.packb(Message(b'VISITED', new_visited).build()))
                # self.pull_socket.recv()
                self.push_socket.send(msgpack.packb(msg))
                self.pull_socket.recv()
                # self.push_socket.send(msgpack.packb(Message(b'DONE', b'').buil()))

    def bfs(self, nodes):
        # if node in self.cache['visited']:
        #     return [], {}
        batch = deque(nodes)
        new_nodes = deque()
        new_visited = set()

        cache_visited = self.visited
        cache_nodes_added = self.nodes_added
        cache_search_edges = self.search_edges
        edges = self.edges
        while batch:
            current = batch.popleft()
            if current in cache_visited: 
                continue
            # Add this check
            elif current not in edges:
                # This node belongs to another thread
                # Still add it to visited to prevent cycles
                # cache_visited.add(current)
                # Add it to new_nodes so master knows about it
                new_nodes.append((current, current))
                continue

            elif current in edges:
                for edge in edges[current]:
                    src = edge.src
                    dest = edge.dest
                    batch.append(src)
                    if dest not in cache_nodes_added:
                        batch.append(dest)
                        cache_nodes_added.add(dest)
                        cache_search_edges[src].append(edge)
                        new_nodes.append((src, dest))
                cache_visited.add(current)
                new_visited.add(current)
        return list(new_nodes), list(new_visited)
