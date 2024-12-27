from collections import deque
import json
import socket

class Master:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

        # nodes → ip kept on server
        self.nodes = dict()
        # list[tuple] to keep track of all client threads
        self.threads = list()

    def recv(self):
        data, _ = self.socket.recvfrom(1024)
        return json.loads(data.decode())

    def send(self, msg, ip, port):
        # `send` awaits for a confirmation message
        self.socket.sendto(json.dumps(msg).encode(), (ip, int(port)))
        answer, address = self.socket.recvfrom(1024)
        return json.loads(answer.decode()), address

    def __get_partition(self, node):
        # hash-based partitioning
        index = hash(node) % len(self.threads)
        return self.threads[index]

    def add_thread(self, ip, port):
        self.threads.append((ip, port))

    def add_node(self, node):
        ip, port = self.__get_partition(node)
        self.nodes[node] = (ip, port)
        return self.send({
            'type': 'add_node',
            'node': node
        }, ip, port)

    def add_edge(self, n1, n2, weight=1):
        return self.send({
            'type': 'add_edge',
            'node1': n1,
            'node2': n2,
            'weight': weight
        }, self.nodes[n1][0], self.nodes[n1][1])

    def get_edges(self, node, **kwargs):
        edges = []
        if node not in self.nodes:
            return None
        self.socket.sendto(json.dumps({
            'type': 'get_edges',
            'node': node,
            'bfs': kwargs.get('bfs'),
            'dfs': kwargs.get('dfs')
        }).encode(), (self.nodes[node][0], self.nodes[node][1]))
        while True:
            data, addr = self.socket.recvfrom(1024)
            self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
            if json.loads(data).get('status') == 'done':
                break
            edges.append(json.loads(data))
        return edges


    def bfs(self, root):
        for thread in self.threads:
            self.send({
                'type': 'init_bfs'
            }, *thread)
        
        nodes = deque([root])
        bfs_tree = {root: []}
        
        while nodes:
            node = nodes.popleft()
            edges = self.get_edges(node, bfs=True)
            
            if edges:
                bfs_tree[node] = []
                destinations = [edge['dest'] for edge in edges if edge['dest'] != node]
                for d in destinations:
                    if bfs_tree.get(d) is None:
                        bfs_tree[d] = []
                        nodes.append(d)
                        bfs_tree[node].append(d)
        
        return bfs_tree

    def dfs(self, root):
        for thread in self.threads:
            self.send({
                'type': 'init_dfs'
            }, *thread)

        nodes = deque([root])
        dfs_tree = {root: []}

        while nodes:
            node = nodes.pop()
            edges = self.get_edges(node, dfs=True)

            if edges:
                if dfs_tree.get(node) is None: dfs_tree[node] = []
                destinations = [edge['dest'] for edge in edges if edge['dest'] != node]
                for d in destinations:
                    if dfs_tree.get(d) is None:
                        dfs_tree[d] = []
                        nodes.append(node)
                        nodes.append(d)
                        dfs_tree[node].append(d)
                        self.send({
                            'type': 'visit_node',
                            'node': d
                        }, *self.nodes[d])
                        self.send({
                            'type': 'visit_node',
                            'node': node
                        }, *self.nodes[node])
                        break

        return dfs_tree