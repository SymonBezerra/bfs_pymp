from collections import deque
import json
import socket

from message import Message
from graph import Node

class Master:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

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

    def __get_partition(self, node):
        # hash-based partitioning
        index = hash(node) % len(self.threads)
        return self.threads[index]

    def add_thread(self, ip, port):
        self.threads.append((ip, port))

    def add_node(self, node):
        ip, port = self.__get_partition(node)
        print(node, port)
        self.nodes[node.encode()] = (ip, port)
        return self.send(Message(b'ADD_NODE', node.encode()), ip, port)

    def add_edge(self, n1, n2, weight=1):
        # return self.send({
        #     'type': 'add_edge',
        #     'node1': n1,
        #     'node2': n2,
        #     'weight': weight
        # }, self.nodes[n1][0], self.nodes[n1][1])
        n1_node = n1.encode()
        return self.send(Message(b'ADD_EDGE', f'{n1} {n2} {weight}'.encode()), self.nodes[n1_node][0], self.nodes[n1_node][1])

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
            edges = self.get_edges(node.label, bfs=True)

            if edges:
                if bfs_tree.get(node) is None: bfs_tree[node] = []
                for edge in edges:
                    if edge == b'': continue
                    _, dest, _ = edge.split(b',')
                    dest_node = Node(dest)
                    if dest_node != node and bfs_tree.get(dest_node) is None:
                        # destinations.append(dest_node)
                        nodes.append(dest_node)
                        bfs_tree[node].append(Node(dest))
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
                for edge in edges:
                    _, dest, _ = edge.split()
                    if Node(dest) != node:
                        destinations.append(Node(dest))
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