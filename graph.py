from dataclasses import dataclass

class Node:
    def __init__(self, label: str, attr=dict()):
        self.label = label
        self.attr = attr

class Edge:
    def __init__(self, src: str, dest: str, weight, attr=dict()):
        self.src = src
        self.dest = dest
        self.weight = weight
        self.attr = attr