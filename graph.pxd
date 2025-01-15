from libcpp.vector cimport vector
from libcpp.string cimport string

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

cdef class Node:
    cdef readonly bytes label
    cdef public dict attr

cdef class Edge:
    cdef readonly bytes src
    cdef readonly bytes dest
    cdef public double weight
    cdef public dict attr

cdef class EdgeVectorIterator:
    cdef vector[EdgeStruct]* _vec
    cdef size_t _pos
    cdef void set_vector(self, vector[EdgeStruct]* vec)

cdef class EdgeVector:
    cdef vector[EdgeStruct]* _vec
    cdef EdgeVectorIterator _iterator

cdef class DistGraph:
    cdef public dict nodes
    cdef str __id
    cdef object __master
    cpdef void add_node(self, object node) except *
    cpdef object bfs(self, object node) except *

cdef class DistGraphPartition:
    cdef public dict nodes
    cdef public object _edges
    cdef readonly str id
    cpdef bint node_present(self, bytes node) except *