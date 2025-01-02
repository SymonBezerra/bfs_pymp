from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
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

        # Add these new attributes
        self.adjacency = {}  # Track edges for partitioning decisions
        self.partition_loads = {}  # Track node count per partition

        # nodes → ip kept on server
        self.nodes = dict()
        # list[tuple] to keep track of all client threads
        self.threads = list()
        self.cache = dict()

    def recv(self, bufsize=1024):
        data, addr = self.socket.recvfrom(bufsize)
        # return json.loads(data.decode())
        return Message(data[:20].strip(), data[20:]), addr

    def send(self, msg, ip, port):
        # `send` awaits for a confirmation message
        self.socket.sendto(msg.build(), (ip, int(port)))
        data, _ = self.socket.recvfrom(1024)
        return Message(data[:20].strip(), data[20:].strip())

    def add_thread(self, ip, port):
        self.threads.append((ip, port))

    def add_node(self, node):
        # ip, port = self.__get_partition(node)
        # print(node, ip, port)
        # self.nodes[node.encode()] = (ip, port)
        # return self.send(Message(b'ADD_NODE', node.encode()), ip, port)
        self.nodes[node.encode()] = None

    def __get_partition(self, node):
        # edge-cut would keep it mostly in one worker, requiring only boundary communications
        node_key = node.encode() if isinstance(node, str) else node
        
        # Initialize partition loads if needed
        if not self.partition_loads:
            self.partition_loads = {i: 0 for i in range(len(self.threads))}
        
        # Check neighbors' partitions
        neighbor_partitions = {}
        neighbors = self.adjacency.get(node_key, set())
        
        for neighbor in neighbors:
            if neighbor in self.nodes and self.nodes[neighbor] is not None:
                partition_idx = self.threads.index(self.nodes[neighbor])
                neighbor_partitions[partition_idx] = neighbor_partitions.get(partition_idx, 0) + 1
        
        if neighbor_partitions:
            # Find partition with most neighbors, considering load
            best_partition = None
            best_score = float('-inf')
            
            for partition_idx, neighbor_count in neighbor_partitions.items():
                # Score combines neighbor count and inverse of current load
                load_factor = self.partition_loads[partition_idx] / max(1, sum(self.partition_loads.values()))
                score = neighbor_count - load_factor
                
                if score > best_score:
                    best_score = score
                    best_partition = partition_idx
                    
            self.partition_loads[best_partition] += 1
            return self.threads[best_partition]
        else:
            # If no neighbors, use least loaded partition
            min_load_idx = min(self.partition_loads.items(), key=lambda x: x[1])[0]
            self.partition_loads[min_load_idx] += 1
            return self.threads[min_load_idx]

    def add_edge(self, n1, n2, weight=1):
        n1_node = n1.encode()
        n2_node = n2.encode()
        
        # Update adjacency information
        if n1_node not in self.adjacency:
            self.adjacency[n1_node] = set()
        if n2_node not in self.adjacency:
            self.adjacency[n2_node] = set()
        
        self.adjacency[n1_node].add(n2_node)
        self.adjacency[n2_node].add(n1_node)
        
        # N1 and N2 have no partition
        if self.nodes[n1_node] is None:
            self.nodes[n1_node] = self.__get_partition(n1)
            self.send(Message(b'ADD_NODE', n1_node), *self.nodes[n1_node])
        if self.nodes[n2_node] is None:
            self.nodes[n2_node] = self.__get_partition(n2)
            self.send(Message(b'ADD_NODE', n2_node), *self.nodes[n2_node])
            
        return self.send(Message(b'ADD_EDGE', f'{n1} {n2} {weight}'.encode()), 
                        self.nodes[n1_node][0], self.nodes[n1_node][1])

    def get_edges(self, node, **kwargs):
        edges = []
        if node not in self.nodes:
            return None
        dfs = kwargs.get('dfs')
        bfs = kwargs.get('bfs')
        header = b'GET_EDGES'
        if dfs: header = b'GET_EDGES_DFS'
        elif bfs: header = b'GET_EDGES_BFS'
        self.socket.sendto(Message(header, node).build(), (self.nodes[node][0], self.nodes[node][1]))
        while True:
            msg, addr = self.recv(8096)
            self.socket.sendto(Message(b'OK', b'').build(), addr)
            if msg.header == b'DONE':
                break
            edges.extend(msg.body.split(b'|'))
        return edges


    def bfs(self, root):
        for thread in self.threads:
            self.send(Message(b'INIT_BFS', b''), *thread)
        root_node = root.encode()
        nodes = deque([Node(root_node)])
        bfs_tree = {}

        while nodes:
            node = nodes.popleft()
            if self.send(Message(b'BFS', node.label), *self.nodes[node.label]).header == b'VISITED':
                continue
            edges = self.get_edges(node.label, bfs=True)

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
                        nodes.append(dest_node)
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