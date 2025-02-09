from collections import defaultdict
import heapq
import uuid

import msgpack

from message import Message

class Node:
    def __init__(self, label: bytes):
        self.label = label

    def __hash__(self):
        return hash(self.label)

    def __repr__(self):
        return str(self.label.decode())

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.label == other.label
        raise TypeError(f'Cannot compare Node and {type(other)}')
class Edge:
    def __init__(self, src: bytes, dest: bytes, weight=1, attr=dict()):
        self.src = src
        self.dest = dest
        self.weight = weight
        self.attr = attr

    def __repr__(self):
        # debug
        return f'{self.src.decode()} -> {self.dest.decode()}'

class DistGraph:
    def __init__(self, master):
        self.nodes = dict()

        self.__id = str(uuid.uuid4())

        self.__master = master

        for thread in self.__master.threads:
            socket = self.__master.threads[thread]
            socket.send(msgpack.packb(Message(b'CREATE_GRAPH', self.__id).build()))
            self.__master.pull_socket.recv()

        self.__master.partition_loads[self.__id] = [(0, partition) for partition in self.__master.threads]
        heapq.heapify(self.__master.partition_loads[self.__id])


    @property
    def id(self):
        return self.__id

    def add_node(self, node):
        if isinstance(node, Node):
            self.nodes[node.label] = None
        elif isinstance(node, bytes):
            self.nodes[node] = None
        elif isinstance(node, str):
            self.nodes[node.encode()] = None
        else:
            raise TypeError('Node must be of type Node, bytes, or str')

    def add_edge(self, n1, n2, weight=1):
        self.__master.add_edge(n1, n2, weight, self)

    def bfs(self, node):
        return self.__master.bfs(node, self)

    def __len__(self):
        return len(self.nodes)
    
    def __del__(self):
        for thread in self.__master.threads:
            socket = self.__master.threads[thread]
            socket.send(msgpack.packb(Message(b'DELETE', self.__id).build()))
            self.__master.pull_socket.recv()
        del self.__master.partition_loads[self.__id]

class DistGraphPartitition:
    def __init__(self, id):
        self.__id = id

        self.edges = defaultdict(list)