from collections import defaultdict, deque
from io import BytesIO
# import socket

import msgpack
import zmq

from graph import Edge, DistGraphPartitition
from message import Message

class Thread:
    def __init__(self, ip, port, master_ip, master_port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)

        self.ip = ip
        self.port = port

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
        self.graphs = dict()

        # cache buffer to keep track of visited nodes
        self.visited = set()
        self.nodes_added = set()
        self.search_edges = defaultdict(deque)
        self.to_visit = dict()

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
            id = msg['body'].decode()
            self.graphs[id] = DistGraphPartitition(id)
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'RESTART':
            self.graphs.clear()
            self.visited.clear()
            self.nodes_added.clear()
            self.search_edges.clear()
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
            self.visited = set()
            self.nodes_added = set()
            self.search_edges = defaultdict(deque)
            self.to_visit = {
                node: False for node in self.graphs[msg['body']['id']].edges
            }
            self.push_socket.send(msgpack.packb(Message(b'OK', b'').build()))
        elif header == b'BFS' or header == b'DFS':
            nodes = msg['body']['nodes']
            id = msg['body']['id']
            src_id = msg['body']['src_id']
            if header == b'BFS':
                self.bfs(nodes, id, src_id)

    # def bfs(self, nodes, id):
    #     graph = self.graphs[id]
    #     batch = deque(nodes)
    #     new_nodes = []
    #     new_visited = set()
    #     poller = zmq.Poller()
    #     poller.register(self.push_socket, zmq.POLLOUT)
    #     def send_updates(new_nodes, new_visited, done=False):
    #         message = {
    #             'NEW_NODES': new_nodes,
    #             'VISITED': list(new_visited),
    #             'DONE': done
    #         }
    #         while True:
    #             events = dict(poller.poll(timeout=1000))
    #             if self.push_socket in events:
    #                 self.push_socket.send(msgpack.packb(Message(b'RESULT', message).build()))
    #                 break

    #     while batch:
    #         current = batch.popleft()
    #         if current in self.visited or current not in graph.edges:
    #             continue

    #         for edge in graph.edges[current]:
    #             if edge.dest not in self.nodes_added:
    #                 batch.append(edge.dest)
    #                 self.nodes_added.add(edge.dest)
    #                 self.search_edges[edge.src].append(edge)
    #                 new_nodes.append((edge.src, edge.dest))
    #         self.visited.add(current)
    #         new_visited.add(current)

    #         if len(new_nodes) >= self.__opt['node_batch']:
    #             send_updates(new_nodes, new_visited)
    #             new_nodes.clear()
    #             new_visited.clear()

    #     send_updates(new_nodes, new_visited, done=True)

    def bfs(self, nodes, id, src_id):
        src_graph = self.graphs[src_id]
        graph = self.graphs[id]
        visited = set()
        cross_nodes = list()
        
        batch = deque(nodes)
        while batch:
            current = batch.popleft()
            
            # Skip if already visited
            if current in self.visited:
                continue
                
            # Handle nodes not in this partition
            if current not in src_graph.edges:
                if current not in self.visited:
                    cross_nodes.append(current)
                continue
                
            # Process the node
            graph.edges[current] = []  # Initialize edges list
            self.visited.add(current)
            visited.add(current)
            
            # Process edges
            for edge in src_graph.edges[current]:
                dest = edge.dest
                if dest not in self.nodes_added:
                    graph.edges[current].append(Edge(edge.src, edge.dest))
                    if dest not in self.visited:
                        batch.append(dest)
                        self.nodes_added.add(dest)

            # Send batch if threshold reached
            if len(visited) >= self.__opt['node_batch']:
                message = {
                    'nodes': list(visited),
                    'thread': (self.ip, self.port),
                    'cross_nodes': cross_nodes
                }
                self.push_socket.send(msgpack.packb(Message(b'VISITED', message).build()))
                visited.clear()
                cross_nodes.clear()

        # Send remaining nodes
        if visited or cross_nodes:
            message = {
                'nodes': list(visited),
                'thread': (self.ip, self.port),
                'cross_nodes': cross_nodes
            }
            self.push_socket.send(msgpack.packb(Message(b'VISITED', message).build()))

        # Signal completion
        self.push_socket.send(msgpack.packb(
            Message(b'DONE', {'thread': (self.ip, self.port)}).build()))