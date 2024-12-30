from dataclasses import dataclass

class Node:
    def __init__(self, label: bytes, attr=dict()):
        self.label = label
        self.attr = attr

    def __hash__(self):
        return hash(self.label)

    def __repr__(self):
        return str(self.label.decode())

class Edge:
    def __init__(self, src: bytes, dest: bytes, weight=1, attr=dict()):
        self.src = src
        self.dest = dest
        self.weight = weight
        self.attr = attr