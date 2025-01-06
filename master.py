from collections import defaultdict
from io import BytesIO
import logging
import socket

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
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)

        self.socket.settimeout(5.0)
        # Set high priority for network traffic
        # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)

        # Set Type of Service (TOS) for QoS
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0x10)  # IPTOS_LOWDELAY

        self.socket.bind((ip, port))

        self.partition_loads = {}  # Track node count per partition

        self.bytes_buffer = BytesIO()

        # nodes → ip kept on server
        self.nodes = dict()
        # list[tuple] to keep track of all client threads
        self.threads = list()

        self.cache = dict()

    def recv(self, bufsize=65507):
        data, addr = self.socket.recvfrom(bufsize)
        # return json.loads(data.decode())
        return Message(data[:20].strip(), data[20:]), addr

    def send(self, msg, ip, port):
        # `send` awaits for a confirmation message
        self.socket.sendto(msg.build(), (ip, int(port)))
        data, _ = self.socket.recvfrom(65507)
        return Message(data[:20].strip(), data[20:].strip())

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
        for node_port in node_buffers:
            LOGGER.info(f'Sending nodes to {node_port}')
            self.bytes_buffer.write(b'ADD_NODES'.ljust(20))
            node_batch = 0
            for node in node_buffers[node_port]:
                self.bytes_buffer.write(node)
                self.bytes_buffer.write(b'|')
                node_batch += 1
                if node_batch == 500:
                    self.socket.sendto(self.bytes_buffer.getvalue(), node_port)
                    self.socket.recvfrom(65507)
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'ADD_NODES'.ljust(20))
                    node_batch = 0
            self.socket.sendto(self.bytes_buffer.getvalue(), node_port)
            self.socket.recvfrom(65507)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()

        for port in buffers:
            LOGGER.info(f'Sending edges to {port}')
            self.bytes_buffer.write(b'ADD_EDGES'.ljust(20))
            batch_count = 0
            for edge in buffers[port]:
                self.bytes_buffer.write(edge)
                self.bytes_buffer.write(b'|')
                batch_count += 1
                if batch_count == 500:
                    self.socket.sendto(self.bytes_buffer.getvalue(), port)
                    self.socket.recvfrom(65507) # await confirmation
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'ADD_EDGES'.ljust(20))
                    batch_count = 0
            self.socket.sendto(self.bytes_buffer.getvalue(), port)
            self.socket.recvfrom(65507) # await confirmation
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()

    def add_thread(self, ip, port):
        thread = (ip, port)
        self.threads.append(thread)
        self.partition_loads[thread] = 0

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

        for thread in self.threads:
            self.socket.sendto(Message(b'INIT_BFS', b'').build(), thread)

        while nodes:
            batches = defaultdict(list)
            for n in nodes:
                if n not in visited: batches[self.nodes[n]].append(n)
            nodes.clear()

            for thread in batches:
                batch_size = 0
                self.bytes_buffer.write(b'BFS'.ljust(20))

                for node in batches[thread]:
                    self.bytes_buffer.write(node)
                    self.bytes_buffer.write(b',')
                    batch_size += 1

                    if batch_size == 500:
                        batch_size = 0
                        self.socket.sendto(self.bytes_buffer.getvalue(), thread)
                        self.bytes_buffer.seek(0)
                        self.bytes_buffer.truncate()
                        self.bytes_buffer.write(b'BFS'.ljust(20))

                        while True:
                            msg, addr = self.recv(65507)
                            self.socket.sendto(Message(b'OK', b'').build(), addr)
                            if msg.header == b'DONE':
                                break
                            elif msg.header == b'NEW_NODES':
                                new_nodes = [nodes for nodes in msg.body.split(b'|') if nodes != b'']
                                for node_tuple in new_nodes:
                                    src, dest = node_tuple.split(b',')
                                    src_node, dest_node = Node(src), Node(dest)
                                    if src_node == dest_node and src_node not in bfs_tree:
                                        nodes.append(src)
                                    elif dest_node not in bfs_tree:
                                        bfs_tree[src_node].append(dest_node)
                                        bfs_tree[dest_node] = []
                                        nodes.append(dest)
                            elif msg.header == b'VISITED':
                                visited_nodes = {node for node in msg.body.split(b',') if node != b''}
                                visited.update(visited_nodes)

                self.socket.sendto(self.bytes_buffer.getvalue(), thread)
                self.bytes_buffer.seek(0)
                self.bytes_buffer.truncate()
                while True:
                    msg, addr = self.recv(65507)
                    self.socket.sendto(Message(b'OK', b'').build(), addr)
                    if msg.header == b'DONE':
                        break
                    elif msg.header == b'NEW_NODES':
                        new_nodes = [nodes for nodes in msg.body.split(b'|') if nodes != b'']
                        for node_tuple in new_nodes:
                            src, dest = node_tuple.split(b',')
                            src_node, dest_node = Node(src), Node(dest)
                            if src_node == dest_node and src_node not in bfs_tree:
                                nodes.append(src)
                            elif dest_node not in bfs_tree:
                                bfs_tree[src_node].append(dest_node)
                                bfs_tree[dest_node] = []
                                nodes.append(dest)
                    elif msg.header == b'VISITED':
                        visited_nodes = {node for node in msg.body.split(b',') if node != b''}
                        visited.update(visited_nodes)
        return bfs_tree

    def restart_threads(self):
        self.nodes.clear()
        for thread in self.threads:
            self.socket.sendto(Message(b'RESTART', b'').build(), thread)
            self.recv()