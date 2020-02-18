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


    ## Attempted backwards but does not catch prereq's among the same level of parents
    def get_ordering_from_end(self, start, end):
        ordering = OrderedDict()
        Q = deque([self.graph.get_node(end)])
        found = False
        while len(Q) > 0:
            node = Q.popleft()

            if (node.get_value() == start):
                found = True
            else:
                ordering[node.get_value()] = None
            if (not found):
                for parent in node.get_parents():
                    if (not parent.is_parent(start) and parent.get_visited() == False):
                        Q.append(parent)
            node.set_visited(True)
        ordering[start] = None
        return list(ordering.keys())[::-1]



    def get_ordering(self, start, end):
        #Using this to avoid duplicates and preserve ordering
        ordering = OrderedDict() 
        Q = deque([self.graph.get_node(start)])

        while len(Q) != 0:
            node = Q.popleft()
            
            if (node.get_value() != start):
                #this conditional is to satisfy:
                ## "if a task is not a prerequisite task of goal, or its task is a 
                # prerequisite task for starting tasks (already been executed), 
                # then it shouldn't be included in the path."

                for parent in node.get_parents():
                    if parent.get_visited() == False:
                        parent.set_visited(True)
                        ordering[parent.get_value()] = None
                
            ordering[node.get_value()] = None

            for child in node.get_children():
                if child.get_visited() == False:
                    Q.append(child)
            
            node.set_visited(True)
            if (node.get_value() == end):
                break
        

        ## Weed out nodes that aren't prerequisites by traversing backwards
        Q = deque([self.graph.get_node(end)])
        while len(Q) > 0:
            node = Q.popleft()
            ordering[node.get_value()] = True
            for parent in node.get_parents():
                # Don't add parents of the starting node
                if (not parent.is_parent(start)):
                    Q.append(parent)
        
        return [key for key, value in ordering.items() if value == True]

if __name__ == "__main__":

    problems = [("task_ids.txt", "relations.txt", "73", "36"), ("small_nodes.txt", "small_edges.txt", "a", "f")]
    for node_file, edge_file, start, end in problems:
        top = TopSort(node_file, edge_file)
        print(top.get_ordering(start, end))
    
