from collections import defaultdict, deque
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
            print(node_port)
            self.bytes_buffer.write(b'ADD_NODES'.ljust(20))
            node_batch = 0
            for node in node_buffers[node_port]:
                self.bytes_buffer.write(node)
                self.bytes_buffer.write(b'|')
                node_batch += 1
                if node_batch == 500:
                    self.socket.sendto(self.bytes_buffer.getvalue(), node_port)
                    self.socket.recv(65507)
                    self.bytes_buffer.seek(0)
                    self.bytes_buffer.truncate()
                    self.bytes_buffer.write(b'ADD_NODES'.ljust(20))
                    node_batch = 0
            self.socket.sendto(self.bytes_buffer.getvalue(), node_port)
            self.socket.recv(65507)
            self.bytes_buffer.seek(0)
            self.bytes_buffer.truncate()

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
            self.send(Message(b'ADD_NODE', src_node), *self.nodes[src_node])
        if self.nodes[dest_node] is None and self.nodes[src_node] is not None:
            self.nodes[dest_node] = self.__get_partition(dest_node, self.nodes[src_node])
            self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
        elif self.nodes[dest_node] is None and self.nodes[src_node] is None:
            self.nodes[dest_node] = self.__get_partition(dest_node)
            self.send(Message(b'ADD_NODE', dest_node), *self.nodes[dest_node])
        return self.send(Message(b'ADD_EDGE', f'{src} {dest} {weight}'.encode()), *self.nodes[src_node])


    # def get_edges(self, node, **kwargs):
    #     edges = []
    #     visited = set()
    #     if node not in self.nodes:
    #         return None
    #     dfs = kwargs.get('dfs')
    #     bfs = kwargs.get('bfs')
    #     header = b'GET_EDGES'
    #     if dfs: header = b'GET_EDGES_DFS'
    #     elif bfs: header = b'GET_EDGES_BFS'
    #     self.socket.sendto(Message(header, node).build(), (self.nodes[node][0], self.nodes[node][1]))
    #     while True:
    #         msg, addr = self.recv(65507)
    #         self.socket.sendto(Message(b'OK', b'').build(), addr)
    #         if msg.header == b'DONE':
    #             break
    #         elif msg.header == b'VISITED':
    #             if msg.body == b'': continue
    #             visited.update({node for node in msg.body.split(b',') if node != b''})
    #         elif msg.header == b'EDGE': 
    #             edges.extend(msg.body.split(b'|'))
    #     return edges, visited


    # def bfs(self, root):
    #     for thread in self.threads:
    #         self.send(Message(b'INIT_BFS', b''), *thread)
    #     root_node = root.encode()
    #     nodes = deque([Node(root_node)])
    #     bfs_tree = defaultdict(list)
    #     visited = set()

    #     while nodes:
    #         node = nodes.popleft()
    #         if node.label in visited: continue
    #         edges, new_visited = self.get_edges(node.label, bfs=True)
    #         visited.update(new_visited)

    #         if edges:
    #             # if bfs_tree.get(node) is None: bfs_tree[node] = []
    #             for edge in edges:
    #                 if edge == b'': continue
    #                 src, dest, _ = edge.split(b',')
    #                 src_node = Node(src)
    #                 dest_node = Node(dest)
    #                 # if bfs_tree.get(src_node) is None:
    #                 #     bfs_tree[src_node] = []
    #                 if dest_node != src_node and dest_node not in bfs_tree:
    #                     # destinations.append(dest_node)
    #                     if dest_node.label not in visited: nodes.append(dest_node)
    #                     # bfs_tree[src_node].append(Node(dest))
    #                     bfs_tree[src_node].append(dest_node)
    #                     bfs_tree[dest_node] = list()
    #             # for d in destinations:
    #             #     if bfs_tree.get(d) is None:
    #             #         bfs_tree[d] = []
    #             #         nodes.append(d)
    #             #         bfs_tree[node].append(d)

    #     return bfs_tree


    def bfs(self, node):
        visited = set()
        nodes = deque([node.encode()]) # deque[bytes]

        for thread in self.threads:
            self.send(Message(b'INIT_BFS', b''), *thread)

        while nodes:
            current = nodes.popleft()
            if current in visited: continue
            self.socket.sendto(Message(b'BFS', current).build(), self.nodes[current])
            # visited.add(current)
            while True:
                msg, addr = self.recv(65507)
                self.socket.sendto(Message(b'OK', b'').build(), addr)
                if msg.header == b'DONE':
                    break
                elif msg.header == b'VISITED':
                    new_nodes = [node for node in msg.body.split(b',') if node != b'']
                    # if not new_nodes: break
                    nodes.extend(new_nodes)
                visited.add(current)

                # elif msg.header == b'EDGE':
                #     edges = msg.body.split(b'|')
                #     for edge in edges:
                #         src, dest, weight = edge.split(b',')
                #         bfs_tree[src].append((dest, weight))
                #         if dest not in visited:
                #             nodes.append(dest)
        # for node in self.nodes:
        #     self.send(Message(b'GET_EDGES_BFS', node), *self.nodes[node])
        #     while True:
        #         msg, addr = self.recv(65507)
        #         self.socket.sendto(Message(b'OK', b'').build(), addr)
        #         print(msg.build())
        #         if msg.header == b'DONE':
        #             break
        #         elif msg.header == b'EDGE':
        #             edges = msg.body.split(b'|')
        #             for edge in edges:
        #                 if edge == b'': continue
        #                 src, dest, weight = edge.split(b',')
        #                 bfs_tree[src].append((dest, weight))
        # return bfs_tree