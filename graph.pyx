import uuid
import heapq
import msgpack

from libcpp.vector cimport vector
from libcpp.string cimport string
from collections import defaultdict
from cython.operator cimport dereference as deref
from cpython.object cimport PyObject
from cpython.bytes cimport PyBytes_Check

from message import Message

cdef class Node:
    cdef readonly bytes label
    cdef public dict attr

    def __cinit__(self, bytes label, dict attr=None):
        if attr is None:
            attr = {}
        if not PyBytes_Check(label):
            raise TypeError("label must be bytes")
            
        self.label = label
        self.attr = attr
    
    def __hash__(self):
        return hash(self.label)
    
    def __repr__(self):
        return self.label.decode('utf-8')
    
    def __eq__(self, object other):
        if isinstance(other, Node):
            return self.label == (<Node>other).label
        raise TypeError(f'Cannot compare Node and {type(other)}')

cdef class Edge:
    cdef readonly bytes src
    cdef readonly bytes dest
    cdef public double weight
    cdef public dict attr
    
    def __cinit__(self, object src, object dest, double weight=1.0, dict attr=None):
        if attr is None:
            attr = {}
            
        # Handle src encoding
        if isinstance(src, bytes):
            self.src = src  # Already bytes, use directly
        else:
            self.src = str(src).encode()
            
        # Handle dest encoding
        if isinstance(dest, bytes):
            self.dest = dest  # Already bytes, use directly
        else:
            self.dest = str(dest).encode()
            
        self.weight = weight
        self.attr = attr
    
    def __repr__(self):
        return f"{self.src.decode('utf-8')} -> {self.dest.decode('utf-8')}"

cdef class DistGraph:
    cdef public dict nodes  # Declare all attributes first
    cdef str __id
    cdef object __master
    
    def __cinit__(self, master):
        self.nodes = {}  # Now we can initialize them
        self.__id = str(uuid.uuid4())
        self.__master = master
        
        threads = getattr(master, 'threads')
        for thread in threads:
            socket = threads[thread]
            socket.send(msgpack.packb(Message(b'CREATE_GRAPH', self.__id.encode()).build()))
            getattr(master, 'pull_socket').recv()
        
        partition_loads = getattr(master, 'partition_loads')
        partition_loads[self.__id] = [(0, partition) for partition in threads]
        heapq.heapify(partition_loads[self.__id])
    
    @property
    def id(self):
        return self.__id
    
    cpdef void add_node(self, object node) except *:
        cdef bytes label
        
        if isinstance(node, Node):
            label = (<Node>node).label
        elif PyBytes_Check(node):
            label = <bytes>node
        elif isinstance(node, str):
            label = (<str>node).encode('utf-8')
        else:
            raise TypeError('Node must be of type Node, bytes, or str')
            
        self.nodes[label] = None
    
    def add_edge(self, n1, n2, weight=1.0):
        cdef bytes label_n1, label_n2
        # We should check if n1 is already bytes
        if isinstance(n1, bytes):
            label_n1 = n1
        elif isinstance(n1, str):
            label_n1 = n1.encode()
        else:
            label_n1 = str(n1).encode()
        
        if isinstance(n2, bytes):
            label_n2 = n2
        elif isinstance(n2, str):
            label_n2 = n2.encode()
        else:
            label_n2 = str(n2).encode()

        self.__master.add_edge(label_n1, label_n2, weight, self)
    
    cpdef object bfs(self, object node) except *:
        return self.__master.bfs(node, self)
    
    def __len__(self):
        return len(self.nodes)

cdef extern from *:
    """
    struct EdgeStruct {
        std::string src;
        std::string dest;
        double weight;
    };
    """
    struct EdgeStruct:
        string src
        string dest
        double weight

cdef class EdgeVectorIterator:
    cdef vector[EdgeStruct]* _vec
    cdef size_t _pos
    
    cdef void set_vector(self, vector[EdgeStruct]* vec):
        self._vec = vec
        self._pos = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self._pos >= self._vec.size():
            raise StopIteration()
        
        cdef EdgeStruct edge = deref(self._vec)[self._pos]
        # Convert std::string back to Python bytes
        cdef bytes src_bytes = edge.src.c_str()[:edge.src.size()]
        cdef bytes dest_bytes = edge.dest.c_str()[:edge.dest.size()]
        
        self._pos += 1
        return (src_bytes, dest_bytes, edge.weight)

cdef class EdgeVector:
    cdef vector[EdgeStruct]* _vec
    cdef EdgeVectorIterator _iterator
    
    def __cinit__(self):
        self._vec = new vector[EdgeStruct]()
        self._iterator = EdgeVectorIterator()
    
    def __dealloc__(self):
        if self._vec != NULL:
            del self._vec
    
    def append(self, bytes src, bytes dest, double weight):
        cdef EdgeStruct edge
        edge.src = src
        edge.dest = dest
        edge.weight = weight
        self._vec.push_back(edge)
    
    def __iter__(self):
        self._iterator.set_vector(self._vec)
        return self._iterator
    
    def __len__(self):
        return self._vec.size()

cdef class DistGraphPartition:
    cdef public dict nodes
    cdef public object _edges  # Changed to object to accept defaultdict
    cdef readonly str id
    
    def __cinit__(self, str graph_id):
        self.nodes = {}
        self._edges = defaultdict(EdgeVector)  # Now this should work
        self.id = graph_id
    
    def add_edge(self, object edge):
        (<EdgeVector>self._edges[edge.src]).append(edge.src, edge.dest, edge.weight)
    
    def add_node(self, bytes node):
        if node not in self._edges:
            self._edges[node] = EdgeVector()
    
    def get_edges(self, bytes node):
        return self._edges[node]

    def node_present(self, bytes node):
        return node in self._edges