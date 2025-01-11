from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import heapq
import logging
# import socket
import threading
import time
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
        id = graph.id
        visited = set()
        root_node = node.encode()
        nodes = [root_node]
        bfs_tree = defaultdict(list)
        poller = zmq.Poller()
        batch_requests = 0
        
        mutex = threading.Lock()
        
        def process_messages(message_queue):
            nonlocal nodes, batch_requests
            while batch_requests > 0 or not message_queue.empty():
                try:
                    body = message_queue.get(timeout=0.1)
                    with mutex:
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
                        
                    message_queue.task_done()
                except queue.Empty:
                    continue
        
        def recv_messages(message_queue):
            nonlocal batch_requests
            while batch_requests > 0:
                events = dict(poller.poll(timeout=10))
                if self.pull_socket in events:
                    try:
                        msg_raw = self.pull_socket.recv(zmq.NOBLOCK)
                        msg = msgpack.unpackb(msg_raw, raw=False)
                        
                        if msg['header'] == b'RESULT':
                            message_queue.put(msg['body'])
                            if msg['body']['DONE']:
                                batch_requests -= 1
                    except zmq.Again:
                        continue
                    except queue.Full:
                        time.sleep(0.001)  # Brief pause if queue is full
        
        # Initialize poller and workers
        poller.register(self.pull_socket, zmq.POLLIN)
        for push_socket in self.threads:
            poller.register(self.threads[push_socket], zmq.POLLOUT)
            socket = self.threads[push_socket]
            socket.send(msgpack.packb(Message(b'INIT_BFS', b'').build()), zmq.NOBLOCK)
            self.pull_socket.recv()
        
        while nodes:
            # Process nodes in batches
            batches = defaultdict(list)
            with mutex:
                for n in nodes:
                    if n not in visited:
                        batches[graph.nodes[n]].append(n)
                nodes.clear()
            
            # Send batches using non-blocking operations
            for thread in batches:
                batch_size = self.__opt['node_batch']
                for i in range(0, len(batches[thread]), batch_size):
                    self.threads[thread].send(msgpack.packb(Message(b'BFS',
                    {
                        'nodes': batches[thread][i:i+batch_size],
                        'id': id
                    }).build()), zmq.NOBLOCK)
                    batch_requests += 1
            
            # Use thread pool for message handling
            recv_future = self.thread_pool.submit(recv_messages, self.message_queue)
            process_future = self.thread_pool.submit(process_messages, self.message_queue)
            
            # Wait for completion
            recv_future.result()
            process_future.result()
            
            # Ensure queue is empty before continuing
            self.message_queue.join()
        
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