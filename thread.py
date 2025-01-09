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

        self.ip = ip
        self.port = port

        self.socket.bind(f'tcp://{ip}:{port}')
        self.socket.setsockopt(zmq.SNDHWM, 10000)  # Send high water mark
        self.socket.setsockopt(zmq.RCVHWM, 10000)  # Receive high water mark
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.socket.setsockopt(zmq.RCVBUF, 8388608)
        self.socket.setsockopt(zmq.SNDBUF, 8388608)
        self.socket.connect(f'tcp://{master_ip}:{master_port}')

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port + 1000}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 10000)
        self.pull_socket.setsockopt(zmq.RCVBUF, 8388608)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect(f'tcp://{master_ip}:{master_port + 1000}')
        self.push_socket.setsockopt(zmq.SNDHWM, 10000)
        self.push_socket.setsockopt(zmq.SNDBUF, 8388608)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # adjacency list, source → destinies kept in the clients
        self.edges = defaultdict(deque)

        # cache buffer to keep track of visited nodess
        self.visited = set()
        self.nodes_added = set()
        self.search_edges = defaultdict(deque)

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
            self.visited.clear()
            self.nodes_added.clear()
            self.search_edges.clear()
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
                self.bfs(nodes)
                # new_nodes, new_visited = self.bfs(nodes)
                # if not new_nodes and not new_visited:
                #     self.push_socket.send(msgpack.packb(Message(b'DONE', b'').build()))
                #     return

                # msg = Message(b'RESULT',
                #     {
                #         'NEW_NODES': new_nodes,
                #         'VISITED': new_visited,
                #         'THREAD_ID': (self.ip, self.port)}).build()

                # self.push_socket.send(msgpack.packb(Message(b'NEW_NODES', new_nodes).build()))
                # self.pull_socket.recv() # await confirmation

                # self.push_socket.send(msgpack.packb(Message(b'VISITED', new_visited).build()))
                # self.pull_socket.recv()
                # self.push_socket.send(msgpack.packb(msg))
                # self.pull_socket.recv()
                # self.push_socket.send(msgpack.packb(Message(b'DONE', b'').build()))

    def bfs(self, nodes):
        batch = deque(nodes)
        new_nodes = []
        new_visited = set()

        while batch:
            current = batch.popleft()
            if current in self.visited or current not in self.edges:
                continue

            for edge in self.edges[current]:
                if edge.dest not in self.nodes_added:
                    batch.append(edge.dest)
                    self.nodes_added.add(edge.dest)
                    self.search_edges[edge.src].append(edge)
                    new_nodes.append((edge.src, edge.dest))
            self.visited.add(current)
            new_visited.add(current)

            # Send batched updates when reaching the batch size
            if len(new_nodes) >= self.__opt['node_batch']:
                self._send_updates(new_nodes, new_visited)
                new_nodes.clear()
                new_visited.clear()

        # # Final batch
        # if new_nodes or new_visited:
        self._send_updates(new_nodes, new_visited, done=True)
        # self.push_socket.send(msgpack.packb(Message(b'DONE', b'').build()))

    def _send_updates(self, new_nodes, new_visited, done=False):
        message = {
            'NEW_NODES': new_nodes,
            'VISITED': list(new_visited),
            'DONE': done
        }
        self.push_socket.send(msgpack.packb(Message(b'RESULT', message).build()))
        self.pull_socket.recv()
