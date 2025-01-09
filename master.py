from collections import defaultdict
from io import BytesIO
import logging
# import socket

import msgpack
import zmq

from message import Message
from graph import Node

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
LOGGER.addHandler(console_handler)

class Master:
    def __init__(self, ip, port):
        self.context = zmq.Context()

        self.socket = self.context.socket(zmq.REQ)
        self.socket.bind(f'tcp://{ip}:{port}')
        self.socket.setsockopt(zmq.SNDHWM, 10000)  # Send high water mark
        self.socket.setsockopt(zmq.RCVHWM, 10000)  # Receive high water mark
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.socket.setsockopt(zmq.RCVBUF, 8388608)
        self.socket.setsockopt(zmq.SNDBUF, 8388608)

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port + 1000}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        self.pull_socket.setsockopt(zmq.RCVBUF, 8388608)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        self.push_sockets = dict()

        self.partition_loads = dict() # Track node count per partition

        self.bytes_buffer = BytesIO()

        # nodes → ip kept on server
        self.nodes = dict()
        # list[tuple] to keep track of all client threads
        self.threads = dict()

        self.__opt = {
            'node_batch': 5000,
            'edge_batch': 5000
        }

    def set_opt(self, opt, value):
        try:
            self.__opt[opt] = value
        except KeyError:
            raise ValueError(f"Invalid option: {opt}")

    def load_file(self, path):
        buffers = {port: [] for port in self.threads}
        node_buffers = {port: [] for port in self.threads}

        with open(path, 'r') as file:

            for line in file:
                src, dest = line.strip().split(' ')
                src_node = src.encode()
                dest_node = dest.encode()

                if self.nodes[src_node] is None:
                    self.nodes[src_node] = self.__get_partition(src_node)
                    # self.send(Message(b'ADD_NODE', src_node), *self.nodes[src_node])
                    node_buffers[self.nodes[src_node]].append(src_node)
                if self.nodes[dest_node] is None and self.nodes[src_node] is not None:
                    self.nodes[dest_node] = self.__get_partition(dest_node, self.nodes[src_node])
                    # self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
                    node_buffers[self.nodes[dest_node]].append(dest_node)
                elif self.nodes[dest_node] is None and self.nodes[src_node] is None:
                    self.nodes[dest_node] = self.__get_partition(dest_node)
                    # self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
                    node_buffers[self.nodes[dest_node]].append(dest_node)
                buffers[self.nodes[src_node]].append(f'{src},{dest},1'.encode())

        for port in buffers:
            push_socket = self.push_sockets[port]
            LOGGER.info(f'Sending edges to {port}')
            for _ in range(0, len(buffers[port]), self.__opt['edge_batch']):
                message = Message(b'ADD_EDGES', buffers[port][_:_+self.__opt['edge_batch']])
                push_socket.send(msgpack.packb(message.build()))
                self.pull_socket.recv()


    def add_thread(self, ip, port):
        thread = (ip, port)
        self.partition_loads[thread] = 0
        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{ip}:{port}')
        socket.setsockopt(zmq.SNDHWM, 1000)  # Send high water mark
        socket.setsockopt(zmq.RCVHWM, 1000)  # Receive high water mark
        socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.threads[thread] = socket

        push_socket = self.context.socket(zmq.PUSH)
        push_socket.connect(f'tcp://{ip}:{port + 1000}')
        push_socket.setsockopt(zmq.SNDHWM, 1000)
        push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.push_sockets[thread] = push_socket

    def add_node(self, node):
        self.nodes[node.encode()] = None

    def __get_partition(self, node, neighbor=None):
        # Get target load per partition
        target_load = len(self.nodes) // len(self.threads)

        if neighbor:
            # If neighbor partition isn't overloaded, prefer it for locality
            if self.partition_loads[neighbor] < target_load:
                self.partition_loads[neighbor] += 1
                return neighbor

            # Find least loaded partition that isn't the neighbor
            min_load = float('inf')
            min_partition = None
            for partition in self.threads:
                if partition != neighbor and self.partition_loads[partition] < min_load:
                    min_load = self.partition_loads[partition]
                    min_partition = partition

            self.partition_loads[min_partition] += 1
            return min_partition

        # No neighbor - assign to least loaded partition
        min_load = float('inf')
        min_partition = None
        for partition in self.threads:
            if self.partition_loads[partition] < min_load:
                min_load = self.partition_loads[partition]
                min_partition = partition

        self.partition_loads[min_partition] += 1
        return min_partition

    def add_edge(self, src, dest, weight=1):
        src_node = src.encode()
        dest_node = dest.encode()
        if self.nodes[src_node] is None:
            self.nodes[src_node] = self.__get_partition(src_node)
            self.socket.sendto(Message(b'ADD_NODE', src_node).build(), self.nodes[src_node])
        if self.nodes[dest_node] is None and self.nodes[src_node] is not None:
            self.nodes[dest_node] = self.__get_partition(dest_node, self.nodes[src_node])
            self.socket.sendto(Message(b'ADD_NODE', dest_node).build(), self.nodes[dest_node])
        elif self.nodes[dest_node] is None and self.nodes[src_node] is None:
            self.nodes[dest_node] = self.__get_partition(dest_node)
            self.socket.sendto(Message(b'ADD_NODE', dest_node).build(), self.nodes[dest_node])
        return self.socket.sendto(Message(b'ADD_EDGE', f'{src} {dest} {weight}'.encode()).build(), self.nodes[src_node])


    def bfs(self, node):
        visited = set()
        root_node = node.encode()
        nodes = [root_node]
        bfs_tree = defaultdict(list)
        poller = zmq.Poller()
        poller.register(self.pull_socket, zmq.POLLIN)

        for thread in self.threads:
            socket = self.threads[thread]
            socket.send(msgpack.packb(Message(b'INIT_BFS', b'').build()))
            socket.recv()

        while nodes:
            batches = defaultdict(list)
            for n in nodes:
                if n not in visited: batches[self.nodes[n]].append(n)
            nodes.clear()

            for thread in batches:
                for i in range(0, len(batches[thread]), self.__opt['node_batch']):
                    self.push_sockets[thread].send(msgpack.packb(Message(b'BFS', batches[thread][i:i+self.__opt['node_batch']]).build()))
                    while True:
                        msg_raw = self.pull_socket.recv()
                        msg = msgpack.unpackb(msg_raw, raw=False)
                        header = msg['header']
                        body = msg['body']
                        if header == b'RESULT':
                            new_nodes = [nodes for nodes in body['NEW_NODES'] if nodes != b'']
                            visited_nodes = {node for node in body['VISITED'] if node != b''}
                            for node_tuple in new_nodes:
                                src, dest = node_tuple
                                src_node, dest_node = Node(src), Node(dest)
                                if src_node == dest_node and src_node not in bfs_tree:
                                    nodes.append(src)
                                elif dest_node not in bfs_tree:
                                    bfs_tree[src_node].append(dest_node)
                                    bfs_tree[dest_node] = []
                                    nodes.append(dest)
                            visited.update(visited_nodes)
                            # self.push_sockets[thread].send(msgpack.packb(Message(b'OK', b'').build()))
                            break
        return bfs_tree

    def restart_threads(self):
        self.nodes.clear()
        for thread in self.threads:
            socket = self.threads[thread]
            socket.send(msgpack.packb(Message(b'RESTART', b'').build()))
            socket.recv()