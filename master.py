from collections import deque
import json
import socket

from message import Message

class Master:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

        # nodes → ip kept on server
        self.nodes = dict()
        # list[tuple] to keep track of all client threads
        self.threads = list()

    def recv(self):
        data, addr = self.socket.recvfrom(1024)
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
        self.nodes[node] = (ip, port)
        return self.send(Message(b'ADD_NODE', node.encode()), ip, port)

    def add_edge(self, n1, n2, weight=1):
        # return self.send({
        #     'type': 'add_edge',
        #     'node1': n1,
        #     'node2': n2,
        #     'weight': weight
        # }, self.nodes[n1][0], self.nodes[n1][1])
        return self.send(Message(b'ADD_EDGE', f'{n1} {n2} {weight}'.encode()), self.nodes[n1][0], self.nodes[n1][1])

    def get_edges(self, node, **kwargs):
        edges = []
        # if node not in self.nodes:
        #     return None
        # self.socket.sendto(json.dumps({
        #     'type': 'get_edges',
        #     'node': node,
        #     'bfs': kwargs.get('bfs'),
        #     'dfs': kwargs.get('dfs')
        # }).encode(), (self.nodes[node][0], self.nodes[node][1]))
        # while True:
        #     data, addr = self.socket.recvfrom(1024)
        #     self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        #     if json.loads(data).get('status') == 'done':
        #         break
        #     edges.append(json.loads(data))
        if node not in self.nodes:
            return None
        dfs = kwargs.get('dfs')
        bfs = kwargs.get('bfs')
        header = b'GET_EDGES'
        if dfs: header = b'GET_EDGES_DFS'
        elif bfs: header = b'GET_EDGES_BFS'
        self.socket.sendto(Message(header, node.encode()).build(), (self.nodes[node][0], self.nodes[node][1]))
        while True:
            msg, addr = self.recv()
            self.socket.sendto(Message(b'OK', b'').build(), addr)
            if msg.header == b'DONE':
                break
            edges.append(msg.body)
        return edges


    def bfs(self, root):
        for thread in self.threads:
            self.send(Message(b'INIT_BFS', b''), *thread)
        
        nodes = deque([root])
        bfs_tree = {root: []}
        
        while nodes:
            node = nodes.popleft()
            edges = self.get_edges(node, bfs=True)
            
            if edges:
                bfs_tree[node] = []
                destinations = []
                for edge in edges:
                    src, dest, _ = edge.split()
                    if dest != node:
                        destinations.append(dest.decode('latin1'))
                        bfs_tree[node].append(dest.decode('latin1'))
                for d in destinations:
                    if bfs_tree.get(d) is None:
                        bfs_tree[d] = []
                        nodes.append(d)
                        bfs_tree[node].append(d)
        
        return bfs_tree

    def dfs(self, root):
        for thread in self.threads:
            self.send(Message(b'INIT_DFS', b''), *thread)

        nodes = deque([root])
        dfs_tree = {root: []}

        while nodes:
            node = nodes.pop()
            edges = self.get_edges(node, dfs=True)

            if edges:
                if dfs_tree.get(node) is None: dfs_tree[node] = []
                destinations = [edge.split()[1] for edge in edges if edge.split()[2] != node]
                for d in destinations:
                    if dfs_tree.get(d.decode()) is None:
                        dfs_tree[d.decode()] = []
                        nodes.append(node.decode())
                        nodes.append(d.decode())
                        dfs_tree[node].append(d.decode())
                        self.send(Message(b'VISIT_NODE', d), *self.nodes[d])
                        self.send(Message(b'VISIT_NODE', node), *self.nodes[node])
                        break

        return dfs_tree