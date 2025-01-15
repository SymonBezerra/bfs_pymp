from collections import deque
from concurrent.futures import ThreadPoolExecutor
import heapq
import logging
import queue

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

        self.partition_loads = {}

        self.__opt = {
            'node_batch': 5000,
            'edge_batch': 5000
        }
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        self.message_queue = queue.Queue()

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
        node_buffers = {port: [] for port in self.threads}

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
                    node_buffers[graph.nodes[src_node]].append(src_node)
                if graph.nodes[dest_node] is None and graph.nodes[src_node] is not None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node, graph.nodes[src_node])
                    node_buffers[graph.nodes[dest_node]].append(dest_node)
                elif graph.nodes[dest_node] is None and graph.nodes[src_node] is None:
                    graph.nodes[dest_node] = self.__get_partition(graph, dest_node)
                    node_buffers[graph.nodes[dest_node]].append(dest_node)
                buffers[graph.nodes[src_node]].append(f'{src},{dest},1'.encode())

        node_batches = 0
        for port in node_buffers:
            push_socket = self.threads[port]
            LOGGER.info(f'Sending nodes to {port}')
            for i in range(0, len(node_buffers[port]), self.__opt['node_batch']):
                message = Message(b'ADD_NODES', {
                    'nodes': node_buffers[port][i:i+self.__opt['node_batch']],
                    'id': graph.id
                })
                push_socket.send(msgpack.packb(message.build()))
                node_batches += 1
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
        # Access the heap for the current graph's partition loads
        partition_heap = self.partition_loads[graph.id]
        target_load = len(graph.nodes) // len(self.threads)

        if neighbor:
            # Check if the neighbor's partition is under the target load
            for i, (load, partition) in enumerate(partition_heap):
                if partition == neighbor:
                    if load < target_load:
                        # Update the neighbor's load and re-heapify
                        partition_heap[i] = (load + 1, neighbor)
                        heapq.heapify(partition_heap)
                        return neighbor
                    break

        # If no neighbor is specified or the neighbor is overloaded, pick the least loaded partition
        min_load, min_partition = heapq.heappop(partition_heap)

        # Assign the node to the least loaded partition and reinsert it into the heap
        heapq.heappush(partition_heap, (min_load + 1, min_partition))
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
        bfs_tree = DistGraph(self)
        bfs_tree_id = bfs_tree.id
        graph_id = graph.id
        root_node = node.encode()

        visited = set()
        pending_nodes = set()  # Track nodes that are being processed

        poller = zmq.Poller()
        for thread in self.threads:
            poller.register(self.threads[thread], zmq.POLLOUT)
        poller.register(self.pull_socket, zmq.POLLIN)
        active_threads = {thread: False for thread in self.threads}

        # Initialize all threads
        for thread in self.threads:
            socket = self.threads[thread]
            socket.send(msgpack.packb(Message(b'INIT_BFS', {'id': graph.id}).build()))
            self.pull_socket.recv()

        # Start with root node
        root_thread = graph.nodes[root_node]
        self.threads[root_thread].send(
            msgpack.packb(
                Message(b'BFS', {
                    'nodes': [root_node],
                    'id': bfs_tree_id,
                    'src_id': graph_id
                }).build()
            )
        )
        active_threads[root_thread] = True
        pending_nodes.add(root_node)
        edge_buffers = {port: [] for port in self.threads}

        while any(active_threads.values()) or pending_nodes:
            events = dict(poller.poll(timeout=1000))
            if self.pull_socket in events:
                msg_raw = self.pull_socket.recv()
                msg = msgpack.unpackb(msg_raw, raw=False)
                header = msg['header']
                body = msg['body']
                
                if header == b'VISITED':
                    nodes = body['nodes']
                    ip, port = body['thread']
                    thread = (ip, port)
                    
                    # Process visited nodes
                    for node in nodes:
                        if node not in visited:
                            visited.add(node)
                            bfs_tree.nodes[node] = thread
                            pending_nodes.discard(node)

                    # Process cross nodes immediately
                    cross_nodes = body['cross_nodes']
                    cross_edges = body['cross_edges']
                    buffers = {port: [] for port in self.threads}
                    
                    for node in cross_nodes:
                        if node not in visited and node not in pending_nodes:
                            target_thread = graph.nodes[node]
                            buffers[target_thread].append(node)
                            pending_nodes.add(node)
                            bfs_tree.nodes[node] = target_thread

                    # Send buffered nodes immediately
                    for target_thread, nodes_to_send in buffers.items():
                        if nodes_to_send:
                            for i in range(0, len(nodes_to_send), self.__opt['node_batch']):
                                batch = nodes_to_send[i:i+self.__opt['node_batch']]
                                self.threads[target_thread].send(
                                    msgpack.packb(Message(b'BFS', {
                                        'nodes': batch,
                                        'id': bfs_tree_id,
                                        'src_id': graph_id
                                    }).build())
                                )
                                active_threads[target_thread] = True

                    for edge in cross_edges:
                        src, dest = edge[0].decode(), edge[1].decode()
                        if dest not in visited:
                            edge_buffers[graph.nodes[src.encode()]].append(f'{src},{dest},1'.encode())


                elif header == b'DONE':
                    ip, port = body['thread']
                    active_threads[(ip, port)] = False

        for port in edge_buffers:
            push_socket = self.threads[port]
            for i in range(0, len(edge_buffers[port]), self.__opt['edge_batch']):
                message = Message(b'ADD_EDGES', {
                    'edges': edge_buffers[port][i:i+self.__opt['edge_batch']],
                    'id': graph.id
                })
                push_socket.send(msgpack.packb(message.build()))
                # self.pull_socket.recv()
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