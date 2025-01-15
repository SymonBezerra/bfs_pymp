from graph cimport Edge, DistGraphPartition

cdef class Thread:
    cdef object context
    cdef str ip
    cdef int port
    cdef public object pull_socket
    cdef public object push_socket
    cdef public dict graphs
    cdef public set visited
    cdef public set nodes_added
    cdef public object search_edges
    cdef public dict to_visit
    cdef dict __opt
    
    cpdef set_opt(self, str opt, int value)
    cpdef dict recv(self)
    cpdef exec(self, bytes msg_raw)
    cpdef bfs(self, list nodes, str id, str src_id)