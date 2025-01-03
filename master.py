from collections import deque
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from random import randint
import socket

from message import Message
from graph import Node

class Master:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
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

        with open(path, 'r') as file:
            for line in file:
                src, dest = line.strip().split(' ')
                src_node = src.encode()
                dest_node = dest.encode()

                if self.nodes[src_node] is None:
                    self.nodes[src_node] = self.__get_partition(src_node)
                    self.send(Message(b'ADD_NODE', src_node), *self.nodes[src_node])
                if self.nodes[dest_node] is None and self.nodes[src_node] is not None:
                    self.nodes[dest_node] = self.__get_partition(dest_node, self.nodes[src_node])
                    self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
                elif self.nodes[dest_node] is None and self.nodes[src_node] is None:
                    self.nodes[dest_node] = self.__get_partition(dest_node)
                    self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
                buffers[self.nodes[src_node]].append(f'{src},{dest},1'.encode())

        for port in buffers:
            self.bytes_buffer.write(b'ADD_EDGES'.ljust(20))
            batch_count = 0
            for edge in buffers[port]:
                self.bytes_buffer.write(edge)
                self.bytes_buffer.write(b'|')
                batch_count += 1
                if batch_count == 500:
                    self.socket.sendto(self.bytes_buffer.getvalue(), port)
                    self.socket.recv(65507) # await confirmation
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'ADD_EDGES'.ljust(20))
                    batch_count = 0
            if batch_count > 0:
                self.socket.sendto(self.bytes_buffer.getvalue(), port)
                self.socket.recv(65507) # await confirmation
                self.bytes_buffer.seek(0)
                self.bytes_buffer.truncate()

    def add_thread(self, ip, port):
        self.threads.append((ip, port))
        self.partition_loads[(ip, port)] = 0

    def add_node(self, node):
        self.nodes[node.encode()] = None

    def __get_partition(self, node, neighbor=None):
        if neighbor:
            if self.partition_loads[neighbor] >= len(self.nodes) // 2:
                index = randint(0, len(self.threads) - 1)
                while self.threads[index] == neighbor:
                    index = randint(0, len(self.threads) - 1)
                self.partition_loads[self.threads[index]] += 1
                return self.threads[index]
            else:
                self.partition_loads[neighbor] += 1
                return neighbor
        index = randint(0, len(self.threads) - 1)
        while self.threads[index] == neighbor or self.partition_loads[self.threads[index]] >= len(self.nodes) // 2:
            index = randint(0, len(self.threads) - 1)
        self.partition_loads[self.threads[index]] += 1
        return self.threads[index]

    def add_edge(self, src, dest, weight=1):
        src_node = src.encode()
        dest_node = dest.encode()
        if self.nodes[src_node] is None:
            self.nodes[src_node] = self.__get_partition(src_node)
            self.send(Message(b'ADD_NODE', src_node), *self.nodes[src_node])
        if self.nodes[dest_node] is None and self.nodes[src_node] is not None:
            self.nodes[dest_node] = self.__get_partition(dest_node, self.nodes[src_node])
            self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
        elif self.nodes[dest_node] is None and self.nodes[src_node] is None:
            self.nodes[dest_node] = self.__get_partition(dest_node)
            self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
        return self.send(Message(b'ADD_EDGE', f'{src} {dest} {weight}'.encode()), *self.nodes[src_node])


    def get_edges(self, node, **kwargs):
        edges = []
        visited = set()
        if node not in self.nodes:
            return None
        dfs = kwargs.get('dfs')
        bfs = kwargs.get('bfs')
        header = b'GET_EDGES'
        if dfs: header = b'GET_EDGES_DFS'
        elif bfs: header = b'GET_EDGES_BFS'
        self.socket.sendto(Message(header, node).build(), (self.nodes[node][0], self.nodes[node][1]))
        while True:
            msg, addr = self.recv(65507)
            self.socket.sendto(Message(b'OK', b'').build(), addr)
            if msg.header == b'DONE':
                break
            elif msg.header == b'VISITED':
                visited.update({node for node in msg.body.split(b',')})
            elif msg.header == b'EDGE': 
                edges.extend(msg.body.split(b'|'))
        return edges, visited


    def bfs(self, root):
        for thread in self.threads:
            self.send(Message(b'INIT_BFS', b''), *thread)
        root_node = root.encode()
        nodes = deque([Node(root_node)])
        bfs_tree = {}
        visited = set()

        while nodes:
            node = nodes.popleft()
            if node.label in visited: continue
            edges, new_visited = self.get_edges(node.label, bfs=True)
            visited.update(new_visited)

            if edges:
                # if bfs_tree.get(node) is None: bfs_tree[node] = []
                for edge in edges:
                    if edge == b'': continue
                    src, dest, _ = edge.split(b',')
                    src_node = Node(src)
                    dest_node = Node(dest)
                    if bfs_tree.get(src_node) is None:
                        bfs_tree[src_node] = []
                    if dest_node != src_node and bfs_tree.get(dest_node) is None:
                        # destinations.append(dest_node)
                        if dest_node.label not in visited: nodes.append(dest_node)
                        bfs_tree[src_node].append(Node(dest))
                        bfs_tree[dest_node] = []
                # for d in destinations:
                #     if bfs_tree.get(d) is None:
                #         bfs_tree[d] = []
                #         nodes.append(d)
                #         bfs_tree[node].append(d)

        return bfs_tree

    def dfs(self, root):
        for thread in self.threads:
            self.send(Message(b'INIT_DFS', b''), *thread)

        self.cache['get_edges'] = dict()

        nodes = deque([Node(root.encode())])
        dfs_tree = {Node(root.encode()): []}

        while nodes:
            node = nodes.pop()
            # if self.send(Message(b'DFS', node.label), *self.nodes[node.label]).header == b'VISITED':
            #     continue
            cached_edges = self.cache['get_edges'].get(node.label)
            if not cached_edges:
                edges = self.get_edges(node.label, dfs=True)
                self.cache['get_edges'][node.label] = edges
            else: edges = cached_edges

            if edges:
                if dfs_tree.get(node) is None: dfs_tree[node] = []
                destinations = []
                with ThreadPoolExecutor(max_workers=10) as executor:
                    destinations = list(executor.map(lambda edge: Node(edge.split(b',')[1]) if edge else None, edges))
                    destinations = list(filter(lambda dest: dest and dest != node, destinations))
                        # bfs_tree[node].append(Node(dest))
                for d in destinations:
                    if dfs_tree.get(d) is None:
                        dfs_tree[d] = []
                        nodes.append(node)
                        nodes.append(d)
                        dfs_tree[node].append(d)
                        self.send(Message(b'VISIT_NODE', d.label), *self.nodes[d.label])
                        self.send(Message(b'VISIT_NODE', node.label), *self.nodes[node.label])
                        break

        return dfs_tree