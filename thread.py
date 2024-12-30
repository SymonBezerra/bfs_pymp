from io import BytesIO
import socket

from graph import Edge, Node
from message import Message

class Thread:
    def __init__(self, ip, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((ip, port))

        # adjacency list, source → destinies kept in the clients
        self.edges = dict()
        # cache buffer to keep track of visited nodess
        self.cache = dict()
        self.bytes_buffer = BytesIO()

    def recv(self):
        data, addr = self.socket.recvfrom(1024)
        # msg = json.loads(data.decode())
        # if msg.get('type') == 'add_node':
        #     node = msg.get('node')
        #     self.edges[node] = []
        #     self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        # elif msg.get('type') == 'add_edge':
        #     n1, n2 = msg.get('node1'), msg.get('node2')
        #     weight = msg.get('weight', 1)
        #     self.edges[n1].append(Edge(n1, n2, weight))
        #     self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        # elif msg.get('type') == 'init_bfs' or msg.get('type') == 'init_dfs':
        #     self.cache['visited'] = set()
        #     self.cache['nodes_added'] = set()
        #     self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        # elif msg.get('type') == 'bfs' or msg.get('type') == 'dfs':
        #     node = msg.get('node')
        #     if node in self.cache['visited']:
        #         self.socket.sendto(json.dumps({'status': 'visited'}).encode(), addr)
        #     else:
        #         self.cache['visited'].add(node)
        #         self.socket.sendto(json.dumps({'status': 'not_visited'}).encode(), addr)
        # elif msg.get('type') == 'get_edges':
        #     node = msg.get('node')
        #     for edge in self.edges[node]:
        #         if msg.get('bfs') and (edge.dest in self.cache['nodes_added']): continue
        #         elif msg.get('dfs') and (edge.dest in self.cache['visited']): continue
        #         elif msg.get('bfs'): self.cache['nodes_added'].add(edge.dest)
        #         self.socket.sendto(json.dumps({
        #             'src': edge.src,
        #             'dest': edge.dest,
        #             'weight': edge.weight,
        #             'attr': edge.attr
        #         }).encode(), addr)
        #         data, _ = self.socket.recvfrom(1024)
        #         if json.loads(data.decode()).get('status') != 'ok':
        #             break
        #     if msg.get('bfs'):
        #         self.cache['visited'].add(node)
        #         self.cache['nodes_added'].add(node)
        #     self.socket.sendto(json.dumps({'status': 'done'}).encode(), addr)
        # elif msg.get('type') == 'visit_node':
        #     node = msg.get('node')
        #     self.cache['visited'].add(node)
        #     self.socket.sendto(json.dumps({'status': 'ok'}).encode(), addr)
        msg = Message(data[:20].strip(), data[20:].strip())
        if msg.header == b'ADD_NODE':
            node = msg.body
            self.edges[node] = []  # Use label (bytes) as key
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        
        elif msg.header == b'ADD_EDGE':
            n1, n2, weight = msg.body.split()
            self.edges[n1].append(Edge(n1, n2, int(weight)))
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        elif msg.header == b'INIT_BFS' or msg.header == b'INIT_DFS':
            self.cache['visited'] = set()
            self.cache['nodes_added'] = set()
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        elif msg.header == b'BFS' or msg.header == b'DFS':
            node = msg.body
            if node in self.cache['visited']:
                self.socket.sendto(Message(b'VISITED', b'').build(), addr)
            else:
                self.cache['visited'].add(node)
                self.socket.sendto(Message(b'NOT_VISITED', b'').build(), addr)
        elif msg.header == b'GET_EDGES' or msg.header == b'GET_EDGES_BFS' or msg.header == b'GET_EDGES_DFS':
            node = msg.body
            count = 0
            for edge in self.edges[node]:
                if msg.header == b'GET_EDGES_BFS' and (edge.dest in self.cache['nodes_added']): continue
                elif msg.header == b'GET_EDGES_DFS' and (edge.dest in self.cache['visited']): continue
                elif msg.header == b'GET_EDGES_BFS': self.cache['nodes_added'].add(edge.dest)
                self.bytes_buffer.write(b'EDGE'.ljust(20))
                self.bytes_buffer.write(edge.src)
                self.bytes_buffer.write(b' ')
                self.bytes_buffer.write(edge.dest)
                self.bytes_buffer.write(b' ')
                self.bytes_buffer.write(str(edge.weight).encode())
                self.socket.sendto(self.bytes_buffer.getvalue(), addr)
                self.bytes_buffer.seek(0)
                self.bytes_buffer.truncate()
                answer, _ = self.socket.recvfrom(1024)
                if Message(answer[:20].strip(), answer[20:]).header != b'OK':
                    break
            if msg.header == b'GET_EDGES_BFS':
                self.cache['visited'].add(node)
                self.cache['nodes_added'].add(node)
            self.socket.sendto(Message(b'DONE', b'').build(), addr)
        elif msg.header == b'VISIT_NODE':
            node = msg.body
            self.cache['visited'].add(node)
            self.socket.sendto(Message(b'OK', b'').build(), addr)
        return data

    def send(self, header, body, ip, port):
        self.socket.sendto(Message(header, body).build(), (ip, int(port)))
        answer, address = self.socket.recvfrom(1024)
        return Message(answer.decode()[:20].strip(), answer.decode()[20:].strip())