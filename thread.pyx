from collections import defaultdict, deque
from libcpp.set cimport set as cpp_set
from libcpp.string cimport string
from cpython.bytes cimport PyBytes_Check
import msgpack
import zmq

from graph cimport Edge, DistGraphPartition

cdef class Thread:
    
    def __cinit__(self, str ip, int port, str master_ip, int master_port):
        self.context = zmq.Context()

        self.ip = ip
        self.port = port

        # Initialize pull socket
        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind(f'tcp://{ip}:{port + 1000}')
        self.pull_socket.setsockopt(zmq.RCVHWM, 10000)
        self.pull_socket.setsockopt(zmq.RCVBUF, 8388608)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pull_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Initialize push socket
        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect(f'tcp://{master_ip}:{master_port + 1000}')
        self.push_socket.setsockopt(zmq.SNDHWM, 10000)
        self.push_socket.setsockopt(zmq.SNDBUF, 8388608)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.push_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)

        # Initialize data structures
        self.graphs = {}
        self.visited = set()
        self.nodes_added = set()
        self.search_edges = defaultdict(deque)
        self.to_visit = {}

        self.__opt = {
            'node_batch': 500,
            'edge_batch': 500
        }
    
    cpdef set_opt(self, str opt, int value):
        if opt not in self.__opt:
            raise ValueError(f"Invalid option: {opt}")
        self.__opt[opt] = value
    
    cpdef dict recv(self):
        cdef bytes data = self.socket.recv()
        return {
            'header': data[:20].strip(),
            'body': data[20:].strip()
        }
    
    cpdef exec(self, bytes msg_raw):
        cdef dict msg = msgpack.unpackb(msg_raw, raw=False)
        cdef bytes header = msg['header']
        cdef bytes node, n1, n2
        cdef bytes weight
        cdef str id
        cdef list nodes, edges
        cdef DistGraphPartition graph
        
        if header == b'CREATE_GRAPH':
            id = msg['body'].decode()
            self.graphs[id] = DistGraphPartition(id)
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'RESTART':
            self.graphs.clear()
            self.visited.clear()
            self.nodes_added.clear()
            self.search_edges.clear()
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'ADD_NODE':
            node = msg['body']['node']
            id = msg['body']['id']
            graph = self.graphs[id]
            graph.add_node(node)
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'ADD_NODES':
            nodes = msg['body']['nodes']
            id = msg['body']['id']
            graph = self.graphs[id]
            for node in nodes:
                graph.add_node(node)
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'ADD_EDGE':
            n1 = msg['body']['n1']
            n2 = msg['body']['n2']
            weight = msg['body']['weight']
            id = msg['body']['id']
            graph = self.graphs[id]
            graph.add_edge(Edge(n1, n2, int(weight)))
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'ADD_EDGES':
            edges = msg['body']['edges']
            id = msg['body']['id']
            graph = self.graphs[id]
            for edge in edges:
                if edge == b'': continue
                n1, n2, weight = edge.split(b',')
                graph.add_edge(Edge(n1, n2, int(weight)))
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'INIT_BFS' or header == b'INIT_DFS':
            self.visited.clear()
            self.nodes_added.clear()
            self.push_socket.send(msgpack.packb({'header': b'OK', 'body': b''}))
            
        elif header == b'BFS' or header == b'DFS':
            nodes = msg['body']['nodes']
            id = msg['body']['id']
            src_id = msg['body']['src_id']
            if header == b'BFS':
                self.bfs(nodes, id, src_id)

    cpdef bfs(self, list nodes, str id, str src_id):
        cdef DistGraphPartition src_graph = self.graphs[src_id]
        cdef DistGraphPartition graph = self.graphs[id]
        cdef set visited = set()
        cdef list cross_nodes = []
        cdef list cross_edges = []
        cdef bytes current
        cdef object batch = deque(nodes)

        self.nodes_added.update(nodes)
        
        while batch:
            current = batch.popleft()
            
            if current in self.visited:
                continue
                
            if not src_graph.node_present(current):
                if current not in self.visited:
                    cross_nodes.append(current)
                continue
                
            graph.add_node(current)
            self.visited.add(current)
            visited.add(current)
            
            for edge in src_graph.get_edges(current):
                src, dest, _ = edge
                if dest in self.nodes_added:
                    continue
                if not src_graph.node_present(dest):
                    cross_edges.append((src, dest))
                    self.nodes_added.add(dest)
                else:
                    graph.add_edge(Edge(src, dest))
                    self.nodes_added.add(dest)
                if dest not in self.visited:
                    batch.append(dest)

            if len(visited) >= self.__opt['node_batch']:
                message = {
                    'nodes': list(visited),
                    'thread': (self.ip, self.port),
                    'cross_nodes': cross_nodes,
                    'cross_edges': cross_edges
                }
                self.push_socket.send(msgpack.packb({'header': b'VISITED', 'body': message}))
                visited.clear()
                cross_nodes.clear()

        if visited or cross_nodes:
            message = {
                'nodes': list(visited),
                'thread': (self.ip, self.port),
                'cross_nodes': cross_nodes,
                'cross_edges': cross_edges
            }
            self.push_socket.send(msgpack.packb({'header': b'VISITED', 'body': message}))

        self.push_socket.send(msgpack.packb(
            {'header': b'DONE', 'body': {'thread': (self.ip, self.port)}}))