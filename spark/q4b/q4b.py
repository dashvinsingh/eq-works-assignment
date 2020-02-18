from collections import deque, OrderedDict

class Node():
    def __init__(self, value, visited=False):
        self.value = value
        self.visited = visited
        self.parents = set()
        self.children = set()

    def add_parent(self, parent):
        """
        inputs: (Node, Node)
        returns: None
        """
        self.parents.add(parent)
    
    def add_child(self, child):
        """
        inputs: (Node, Node)
        returns: None
        """
        self.children.add(child)
    
    def get_value(self):
        return self.value

    def get_visited(self):
        return self.visited
    
    def get_parents(self):
        return self.parents
    
    def get_children(self):
        return self.children

    def set_visited(self, visted=True):
        """
        Visits (i.e. sets the node to true)
        inputs: Optional(visited)
        returns: Visited value
        """
        self.visited = visted
        return self.visited

    def is_parent(self, node_value):
        """
            Return True if node_value is a child of self (i.e. self is a parent of node_value)
        """
        for node in self.children:
            if node.get_value() == node_value:
                return True
        return False

    def __repr__(self):
        return "Node: {}, {}".format(str(self.get_value()), str(self.get_visited()))

    
class Graph():
    def __init__(self, nodes, edges):
        self.nodes = self.build_nodes(nodes)
        self.build_graph(edges)

    def build_nodes(self, nodes):
        """
        inputs: (nodes) -> list of node values
        returns: empty adjacency list
        """
        node_list = {}
        for node in nodes:
            node_list[node] = Node(node)
        return node_list

    def build_graph(self, edges):
        for edge in edges:
            source, dest = edge.strip().split("->")
            self.add_edge(source, dest)

    def add_edge(self, source, dest):
        self.nodes[source].add_child(self.nodes[dest])
        self.nodes[dest].add_parent(self.nodes[source])
    
    def get_node(self, value):
        return self.nodes[value]

class TopSort():
    def __init__(self, tasks, relations):
        with open(tasks, 'r') as f:
            self.tasks = f.read().split(",")
        
        with open(relations, 'r') as f:
            self.relations = f.readlines()

        self.graph = Graph(self.tasks, self.relations)


    ## Traverse Backward to each parent and so fourth until we reach "start"
    def get_ordering_from_end(self, start, end):

        lst = []
        # Purpose of this is to get all the dependecies of the start node
        self._get_ordering_from_end(start=start, end=None, visited=lst)
        # The final element of the list `lst` is `start`, we want to keep that and remove all it's dependencies (see line 121)
        final_element = len(lst) - 1
        # Gets all the dependencies of the end node, which must include the start
        self._get_ordering_from_end(start=end, end=start, visited=lst)
        return lst[final_element:]
    
    def _get_ordering_from_end(self, start, end, visited=[]):
        """
        Idea: Visit parent before you visit the current node. That is all the dependencies will be "processed" before
        the current package is "processed".
        """
        if start in visited:
            return
        
        if start is not end:
            for parent in self.graph.get_node(start).get_parents():
                self._get_ordering_from_end(start=parent.get_value(), end=end, visited=visited)
        
        visited.append(start)



if __name__ == "__main__":

    problems = [("task_ids.txt", "relations.txt", "73", "36"), 
                ("task_ids.txt", "relations.txt", "73", "37"),
                ("small_nodes.txt", "small_edges.txt", "a", "f"),
                ]
    for node_file, edge_file, start, end in problems:
        top = TopSort(node_file, edge_file)
        print("Start: {}, end: {}, ordering: {}".format(start, end, top.get_ordering_from_end(start, end)))
    
