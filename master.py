from collections import defaultdict, deque
import logging
# import socket
import threading
import time

import msgpack
import zmq

from message import Message
from graph import Node, DistGraph

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

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port + 1000}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 1000)
        self.pull_socket.setsockopt(zmq.RCVBUF, 8388608)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # (ip, port) -> push socket
        self.threads = dict()

        self.partition_loads = dict() # Track node count per partition

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
        graph = DistGraph(self)
        poller = zmq.Poller()
        for thread in self.threads:
            poller.register(self.threads[thread], zmq.POLLOUT)
        poller.register(self.pull_socket, zmq.POLLIN)
        buffers = {port: [] for port in self.threads}

        with open(path, 'r') as file:

            for line in file:
                src, dest = line.strip().split(' ')
                src_node = src.encode()
                dest_node = dest.encode()
                graph.add_node(src_node)
                graph.add_node(dest_node)

            file.seek(0)

            for line in file:
                src, dest = line.strip().split(' ')
                src_node = src.encode()
                dest_node = dest.encode()

                if graph.nodes[src_node] is None:
                    graph.nodes[src_node] = self.__get_partition(graph, src_node)
                if graph.nodes[dest_node] is None and graph.nodes[src_node] is not None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node, graph.nodes[src_node])
                elif graph.nodes[dest_node] is None and graph.nodes[src_node] is None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node)
                buffers[graph.nodes[src_node]].append(f'{src},{dest},1'.encode())
        edge_batches = 0
        for port in buffers:
            push_socket = self.threads[port]
            LOGGER.info(f'Sending edges to {port}')
            for i in range(0, len(buffers[port]), self.__opt['edge_batch']):
                message = Message(b'ADD_EDGES', {
                    'edges': buffers[port][i:i+self.__opt['edge_batch']],
                    'id': graph.id
                })
                push_socket.send(msgpack.packb(message.build()))
                edge_batches += 1

        events = dict(poller.poll(timeout=1000))
        while edge_batches > 0:
            if self.pull_socket in events:
                self.pull_socket.recv()
                edge_batches -= 1
        return graph

    def add_thread(self, ip, port):
        thread = (ip, port)
        push_socket = self.context.socket(zmq.PUSH)
        push_socket.connect(f'tcp://{ip}:{port + 1000}')
        push_socket.setsockopt(zmq.SNDHWM, 1000)
        push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.threads[thread] = push_socket

        self.context.set(zmq.IO_THREADS, len(self.threads) * 2)

    def __get_partition(self, graph, node, neighbor=None):
        # Get target load per partition
        partition_loads = self.partition_loads[graph.id]
        target_load = len(graph.nodes) // len(self.threads)

        if neighbor:
            # If neighbor partition isn't overloaded, prefer it for locality
            if partition_loads[neighbor] < target_load:
                partition_loads[neighbor] += 1
                return neighbor

            # Find least loaded partition that isn't the neighbor
            min_load = float('inf')
            min_partition = None
            for partition in self.threads:
                if partition != neighbor and partition_loads[partition] < min_load:
                    min_load = partition_loads[partition]
                    min_partition = partition

            partition_loads[min_partition] += 1
            return min_partition

        # No neighbor - assign to least loaded partition
        min_load = float('inf')
        min_partition = None
        for partition in self.threads:
            if partition_loads[partition] < min_load:
                min_load = partition_loads[partition]
                min_partition = partition

        partition_loads[min_partition] += 1
        return min_partition

    def add_edge(self, src, dest, weight, graph):
        id = graph.id
        src_node = src.encode()
        dest_node = dest.encode()
        if graph.nodes[src_node] is None:
            graph.nodes[src_node] = self.__get_partition(graph, src_node)

            socket = self.threads[graph.nodes[src_node]]
            socket.send(msgpack.packb(Message(b'ADD_NODE', {
                'id': id,
                'node': src_node
            }).build()))
            self.pull_socket.recv()
        if graph.nodes[dest_node] is None and graph.nodes[src_node] is not None:
            graph.nodes[dest_node] = self.__get_partition(graph, dest_node, graph.nodes[src_node])
            
            socket = self.threads[graph.nodes[dest_node]]
            socket.send(msgpack.packb(Message(b'ADD_NODE', {
                'id': id,
                'node': dest_node
            }).build()))
            self.pull_socket.recv()
        elif graph.nodes[dest_node] is None and graph.nodes[src_node] is None:
            graph.nodes[dest_node] = self.__get_partition(graph, dest_node)
            socket = self.threads[graph.nodes[dest_node]]
            socket.send(msgpack.packb(Message(b'ADD_NODE', {
                'id': id,
                'node': dest_node
            }).build()))
            self.pull_socket.recv()
        socket = self.threads[graph.nodes[src_node]]
        socket.send(msgpack.packb(Message(b'ADD_EDGE', {
            'id': id,
            'n1': src_node,
            'n2': dest_node,
            'weight': weight
        }).build()))
        self.pull_socket.recv()


    def bfs(self, node, graph):
        id = graph.id
        visited = set()
        root_node = node.encode()
        nodes = [root_node]
        bfs_tree = defaultdict(list)
        poller = zmq.Poller()
        batch_requests = 0
        
        mutex = threading.Lock()
        buffer = deque()
        processing_complete = threading.Event()
        receiving_complete = threading.Event()

        def process_message():
            nonlocal nodes
            while not (receiving_complete.is_set() and len(buffer) == 0):
                with mutex:
                    if buffer:
                        body = buffer.popleft()
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
                time.sleep(0.001)  # Small sleep to prevent CPU spinning
            processing_complete.set()

        def recv_message():
            nonlocal batch_requests
            while batch_requests > 0:
                events = dict(poller.poll(timeout=10))
                if self.pull_socket in events:
                    try:
                        msg_raw = self.pull_socket.recv(zmq.NOBLOCK)
                        msg = msgpack.unpackb(msg_raw, raw=False)
                        header = msg['header']
                        body = msg['body']

                        if header == b'RESULT':
                            with mutex:
                                buffer.append(body)
                            if body['DONE']:
                                batch_requests -= 1
                    except zmq.Again:
                        continue
            receiving_complete.set()

        # Initialize poller
        poller.register(self.pull_socket, zmq.POLLIN)
        for push_socket in self.threads:
            poller.register(self.threads[push_socket], zmq.POLLOUT)

        # Initialize BFS workers
        for thread in self.threads:
            socket = self.threads[thread]
            socket.send(msgpack.packb(Message(b'INIT_BFS', b'').build()), zmq.NOBLOCK)
            self.pull_socket.recv()

        while nodes:
            # Reset events for new iteration
            receiving_complete.clear()
            processing_complete.clear()
            
            # Process nodes in batches
            batches = defaultdict(list)
            with mutex:
                for n in nodes:
                    if n not in visited:
                        batches[graph.nodes[n]].append(n)
                nodes.clear()

            # Send batches
            for thread in batches:
                for i in range(0, len(batches[thread]), self.__opt['node_batch']):
                    self.threads[thread].send(msgpack.packb(Message(b'BFS',
                    {
                        'nodes': batches[thread][i:i+self.__opt['node_batch']],
                        'id': id
                    }).build()), zmq.NOBLOCK)
                    batch_requests += 1

            # Start receiver and processor threads
            recv_thread = threading.Thread(target=recv_message)
            process_thread = threading.Thread(target=process_message)

            recv_thread.start()
            process_thread.start()

            # Wait for both threads to complete
            recv_thread.join()
            process_thread.join()

        return bfs_tree

    def dict_to_graph(self, graph_dict):
        poller = zmq.Poller()
        for thread in self.threads:
            poller.register(self.threads[thread], zmq.POLLOUT)
        poller.register(self.pull_socket, zmq.POLLIN)
        graph = DistGraph(self)
        edges = []
        buffers = {port: [] for port in self.threads}
        for node in graph_dict:
            src_node = node.label
            if src_node not in graph.nodes: graph.add_node(node)
            for neighbor in graph_dict[node]:
                dest_node = neighbor.label
                if dest_node not in graph.nodes: graph.add_node(neighbor)
                if graph.nodes[src_node] is None:
                    graph.nodes[src_node] = self.__get_partition(graph, src_node)
                if graph.nodes[dest_node] is None and graph.nodes[src_node] is not None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node, graph.nodes[src_node])
                elif graph.nodes[dest_node] is None and graph.nodes[src_node] is None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node)
                buffers[graph.nodes[src_node]].append(f'{node},{neighbor},1'.encode())
        edge_batches = 0
        for port in buffers:
            push_socket = self.threads[port]
            LOGGER.info(f'Sending edges to {port}')
            for i in range(0, len(buffers[port]), self.__opt['edge_batch']):
                message = Message(b'ADD_EDGES', {
                    'edges': buffers[port][i:i+self.__opt['edge_batch']],
                    'id': graph.id
                })
                push_socket.send(msgpack.packb(message.build()))
                edge_batches += 1

        events = dict(poller.poll(timeout=1000))
        while edge_batches > 0:
            if self.pull_socket in events:
                self.pull_socket.recv()
                edge_batches -= 1
        return graph

    def restart_threads(self):
        self.partition_loads.clear()
        for thread in self.threads:
            socket = self.threads[thread]
            socket.send(msgpack.packb(Message(b'RESTART', b'').build()))
            self.pull_socket.recv()