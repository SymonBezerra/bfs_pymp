import json
import socket

from graph import Edge

class Thread:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

        # adjacency list, source → destinies kept in the clients
        self.edges = dict()
        # cache buffer to keep track of visited nodess
        self.buffer = dict()

    def recv(self):
        data, addr = self.socket.recvfrom(1024)
        msg = json.loads(data.decode())
        if msg.get('type') == 'add_node':
            node = msg.get('node')
            self.edges[node] = []
            self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        elif msg.get('type') == 'add_edge':
            n1, n2 = msg.get('node1'), msg.get('node2')
            weight = msg.get('weight', 1)
            self.edges[n1].append(Edge(n1, n2, weight))
            self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        elif msg.get('type') == 'init_bfs' or msg.get('type') == 'init_dfs':
            self.buffer['visited'] = set()
            self.buffer['nodes_added'] = set()
            self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        elif msg.get('type') == 'bfs' or msg.get('type') == 'dfs':
            node = msg.get('node')
            if node in self.buffer['visited']:
                self.socket.sendto(json.dumps({'status': 'visited'}).encode(), addr)
            else:
                self.buffer['visited'].add(node)
                self.socket.sendto(json.dumps({'status': 'not_visited'}).encode(), addr)
        elif msg.get('type') == 'get_edges':
            node = msg.get('node')
            for edge in self.edges[node]:
                if msg.get('bfs') and (edge.dest in self.buffer['nodes_added']): continue
                elif msg.get('dfs') and (edge.dest in self.buffer['visited']): continue
                elif msg.get('bfs'): self.buffer['nodes_added'].add(edge.dest)
                self.socket.sendto(json.dumps({
                    'src': edge.src,
                    'dest': edge.dest,
                    'weight': edge.weight,
                    'attr': edge.attr
                }).encode(), addr)
                data, _ = self.socket.recvfrom(1024)
                if json.loads(data.decode()).get('status') != 'ok':
                    break
            if msg.get('bfs'):
                self.buffer['visited'].add(node)
                self.buffer['nodes_added'].add(node)
            self.socket.sendto(json.dumps({'status': 'done'}).encode(), addr)
        elif msg.get('type') == 'visit_node':
            node = msg.get('node')
            self.buffer['visited'].add(node)
            self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        return data

    def send(self, msg, ip, port):
        self.socket.sendto(json.dumps(msg).encode(), (ip, int(port)))
        answer, address = self.socket.recvfrom(1024)
        return json.loads(answer.decode()), address