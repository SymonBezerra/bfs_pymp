from collections import defaultdict, deque
from io import BytesIO
# import socket

import msgpack
import zmq

from graph import Edge, DistGraphPartitition
from message import Message

class Thread:
    def __init__(self, ip, port, master_ip, master_port, req_port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)

        self.ip = ip
        self.port = port

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        self.pull_socket.setsockopt(zmq.RCVBUF, 1024 * 1024)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect(f'tcp://{master_ip}:{master_port}')
        self.push_socket.setsockopt(zmq.SNDHWM, 1000)
        self.push_socket.setsockopt(zmq.SNDBUF, 1024 * 1024)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(f'tcp://{master_ip}:{req_port}')
        self.req_socket.setsockopt(zmq.RCVHWM, 1000)
        self.req_socket.setsockopt(zmq.RCVBUF, 1024 * 1024)
        self.req_socket.setsockopt(zmq.SNDHWM, 1000)
        self.req_socket.setsockopt(zmq.SNDBUF, 1024 * 1024)
        self.req_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.req_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # adjacency list, source → destinies kept in the clients
        self.graphs = dict()

        # cache buffer to keep track of visited nodes
        self.visited = set()
        self.nodes_added = set()
        self.root_node = None

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
        if header == b'CREATE_GRAPH':
            id = msg['body']
            self.graphs[id] = DistGraphPartitition(id)
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'DELETE':
            body = msg['body']
            del self.graphs[body]
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'RESTART':
            self.graphs.clear()
            self.visited.clear()
            self.nodes_added.clear()
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_NODE':
            node = msg['body']['node']
            id = msg['body']['id']
            graph = self.graphs[id]
            graph.edges[node] = []
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_NODES':
            nodes = msg['body']['nodes']
            id = msg['body']['id']
            graph = self.graphs[id]
            for node in nodes:
                graph.edges[node] = []
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_EDGE':
            n1 = msg['body']['n1']
            n2 = msg['body']['n2']
            weight = msg['body']['weight']
            id = msg['body']['id']
            graph = self.graphs[id]
            graph.edges[n1].append(Edge(n1, n2, weight))
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'ADD_EDGES':
            edges = msg['body']['edges']
            id = msg['body']['id']
            graph = self.graphs[id]
            for edge in edges:
                if edge == b'': continue
                n1, n2, weight = edge.split(b',')
                graph.edges[n1].append(Edge(n1, n2, int(weight)))
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'INIT_BFS' or header == b'INIT_DFS':
            root_node = msg['body']['root_node']
            self.root_node = root_node
            self.visited = set()
            self.nodes_added = set()
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'BFS' or header == b'DFS':
            nodes = msg['body']['nodes']
            id = msg['body']['id']
            src_id = msg['body']['src_id']
            if header == b'BFS':
                self.bfs(nodes, id, src_id)

    def bfs(self, nodes, id, src_id):
        src_graph = self.graphs[src_id]
        graph = self.graphs[id]
        visited = set()
        cross_nodes = set()
        cross_edges = defaultdict(deque)

        batch = deque(nodes)
        self.nodes_added.update(nodes)

        while batch:
            current = batch.popleft()

            # Skip if already visited
            if current in self.visited:
                continue

            # Handle nodes not in this partition
            if current not in src_graph.edges and current not in self.visited:
                cross_nodes.add(current)
                continue

            # Process the node
            graph.edges[current] = []  # Initialize edges list
            self.visited.add(current)
            visited.add(current)

            # Process edges
            for edge in src_graph.edges[current]:
                src = edge.src
                dest = edge.dest
                if dest in self.nodes_added: continue
                if dest not in src_graph.edges:  # cross-partition edge
                    cross_nodes.add(dest)
                    cross_edges[dest].append(Edge(src, dest))
                else:  # if destination is in this partition
                    graph.edges[src].append(Edge(src, dest))
                    self.nodes_added.add(dest)

                if dest not in self.visited:
                    batch.append(dest)

            # Send batch if threshold reached
            if len(visited) >= self.__opt['node_batch']:
                message = {
                    'nodes': list(visited),
                    'thread': (self.ip, self.port),
                    'cross_nodes': list(cross_nodes)
                }
                self.req_socket.send(msgpack.packb(Message(b'VISITED', message).build()))
                msg_raw = self.req_socket.recv()
                msg = msgpack.unpackb(msg_raw, raw=False)
                if msg['header'] == b'OK':
                    body =  msg['body']
                    for node in body:
                        if body[node]:
                            edges = cross_edges.get(node, [])
                            for edge in edges:
                                dest = edge.dest
                                if dest not in self.nodes_added:
                                    graph.edges[edge.src].append(edge)
                                    self.nodes_added.add(dest)
                    visited.clear()
                    cross_nodes.clear()
                    cross_edges.clear()

        # Send remaining nodes
        if visited or cross_nodes:
            message = {
                'nodes': list(visited),
                'thread': (self.ip, self.port),
                'cross_nodes': list(cross_nodes)
            }
            self.req_socket.send(msgpack.packb(Message(b'VISITED', message).build()))
            msg_raw = self.req_socket.recv()
            msg = msgpack.unpackb(msg_raw, raw=False)
            if msg['header'] == b'OK':
                body =  msg['body']
                for node in body:
                    if body[node]:
                        edges = cross_edges.get(node, [])
                        for edge in edges:
                            dest = edge.dest
                            if dest not in self.nodes_added:
                                graph.edges[edge.src].append(edge)
                                self.nodes_added.add(dest)
            visited.clear()
            cross_nodes.clear()
            cross_edges.clear()

        # Signal completion
        self.push_socket.send(msgpack.packb(
            Message(b'DONE', {'thread': (self.ip, self.port)}).build()))