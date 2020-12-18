import networkx as nx # type:ignore
import matplotlib.pyplot as plt # type:ignore
import random


def select_edges_by_relationship(graph, edge_filter = [], remove = False):
    """
    Filters the edges of a graph.

    Parameters
        graph: Graph to filter
        edge_filter: List of edge relationships to filter on
        remove: whether to remove the selected Edges (optional, default: False) 

    Returns a filtered graph.
    """
    working_graph = graph.copy()
    for edge_id in graph.edges:
        value = working_graph.edges()[edge_id].get('relationship')
        if remove:
            if value in edge_filter:
                working_graph.remove_edge(edge_id[0], edge_id[1])
        else:
            if not value in edge_filter:
                working_graph.remove_edge(edge_id[0], edge_id[1])
    return working_graph


def select_nodes_by_type(graph, node_filter = [], remove = False):
    """
    Filters the edges of a graph based on node_type.

    Parameters
        graph: Graph to filter
        node_filter: List of 'node_types' to filter on
        remove: whether to remove the selected Nodes (optional, default: False)

    Returns a filtered graph.
    """
    if remove:
        result_nodes = filter(lambda x: (x[1].get('node_type') or '-NONE-') in node_filter, graph.nodes(data=True))
    else:
        result_nodes = filter(lambda x: (x[1].get('node_type') or '-NONE-') not in node_filter, graph.nodes(data=True))
    result_nodeids = map(lambda x: x[0], result_nodes)    
    working_graph = graph.copy()
    working_graph.remove_nodes_from(result_nodeids)
    return working_graph
    

def intersect_lists(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 


def intersect_list_of_lists(list_of_lists):
    lister = list_of_lists[0]
    for i in range(len(list_of_lists)):
        lister = intersect_lists(lister, list_of_lists[i])
    return lister


def search_nodes(graph, conditions = {}):
    """
    Filters the edges of a graph based on node_type.

    Parameters
        graph: Graph to filter
        predicate: function to used to search

    Returns a graph.
    """
    target_nodes = []
    for key in conditions:
        _target_nodes = [x for x,y in graph.nodes(data=True) if y.get(key) == conditions[key]]
        target_nodes.append(_target_nodes)
    target_nodes = intersect_list_of_lists(target_nodes)
    return graph.subgraph(target_nodes).copy()


def remove_orphans(graph, node_types=[]):
    """
    Identifies nodes with no edges and removes them.

    Returns a graph.
    """
    working_graph = graph.copy()
    orphan_nodes = []
    for node_id in working_graph.nodes():
        node = working_graph.nodes()[node_id]
        if node.get('node_type') in node_types:
            if len(working_graph.in_edges(node_id)) + len(working_graph.out_edges(node_id)) == 0:
                orphan_nodes.append(node_id)
    for node_id in orphan_nodes:
        working_graph.remove_node(node_id)
    return working_graph


def walk_from(graph, starting_nodes, depth=25, reverse=False):
    """
    Walks a graph from a set of starting nodes, joins the resultant graphs together.

    Parameters
        graph: Graph to filter
        starting_nodes: list of nodes from which to start the walk
        depth: depth limit for breadth-first search
        reverse: flag for breadth-first-search to follow edges in reverse direction

    Returns a graph.
    """
        
    nodes = []
    for node in starting_nodes:
        try:
            tree = nx.bfs_tree(graph, source=node, depth_limit=depth, reverse=reverse)
            nodes = nodes + list(tree.nodes())
        except nx.NetworkXError:
            pass
    return graph.subgraph(nodes).copy()


def find_nearest(graph, start_node, target_node_type, limit=25, reverse=False):
    distance = 1
    while distance < limit:
        paths = walk_from(graph, [start_node], distance, reverse=reverse)
        nodes = select_nodes_by_type(paths, [target_node_type])
        if len(nodes.nodes()) > 0:
            return distance, nodes.nodes()
        distance = distance + 1
    return -1, []



def show_graph(graph, show_edge_labels=False):
    """
    Display a graph

    Parameters
        graph: Graph to display
    """
    LARGE_FONT = 14
    plt.rc('font', size=LARGE_FONT)
    node_labels = nx.get_node_attributes(graph, 'display_name')
    pos = nx.spring_layout(graph, iterations=20)
    plt.figure(figsize = (15,12))
    nx.draw(graph, pos=pos, edge_color="#CCCCCC", linewidths=0.3, node_size=1, with_labels=True, labels=node_labels)
    if show_edge_labels:
        edge_labels = nx.get_edge_attributes(graph, 'relationship')
        nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_color='red')
    plt.axis('off')
    plt.show()


def hierarchy_pos(G, root=None, width=1.5, vert_gap = 1., vert_loc = 0, xcenter = 0.5):

    '''
    From Joel's answer at https://stackoverflow.com/a/29597209/2966723.  
    Licensed under Creative Commons Attribution-Share Alike 

    If the graph is a tree this will return the positions to plot this in a 
    hierarchical layout.

    G: the graph (must be a tree)

    root: the root node of current branch 
    - if the tree is directed and this is not given, 
      the root will be found and used
    - if the tree is directed and this is given, then 
      the positions will be just for the descendants of this node.
    - if the tree is undirected and not given, 
      then a random choice will be used.

    width: horizontal space allocated for this branch - avoids overlap with other branches

    vert_gap: gap between levels of hierarchy

    vert_loc: vertical location of root

    xcenter: horizontal location of root
    '''

    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(iter(nx.topological_sort(G)))  #allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes)) #nosec - CYBASIMP-156

    def _hierarchy_pos(G, root, width=1., vert_gap = 0.2, vert_loc = 0, xcenter = 0.5, pos = None, parent = None):
        '''
        see hierarchy_pos docstring for most arguments

        pos: a dict saying where all nodes go if they have been assigned
        parent: parent of this branch. - only affects it if non-directed

        '''

        if pos is None:
            pos = {root:(xcenter,vert_loc)}
        else:
            pos[root] = (xcenter, vert_loc)
        children = list(G.neighbors(root))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)  
        if len(children)!=0:
            dx = width/len(children) 
            nextx = xcenter - width/2 - dx/2
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(G,child, width = dx, vert_gap = vert_gap, 
                                    vert_loc = vert_loc-vert_gap, xcenter=nextx,
                                    pos=pos, parent = root)
        return pos


    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)


def show_tree(graph):
    pos = hierarchy_pos(graph)
    for node in pos:
        pos[node] = (1 - pos[node][1]),pos[node][0]
    
    LARGE_FONT = 12
    plt.rc('font', size=LARGE_FONT)
    
    node_labels = nx.get_node_attributes(graph, 'display_name')
    plt.figure(figsize = (25,16))
    nx.draw(graph, pos=pos, edge_color="#CCCCCC", linewidths=0.3, node_size=1, with_labels=True, labels=node_labels)
    edge_labels = nx.get_edge_attributes(graph, 'relationship')
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_color='red')