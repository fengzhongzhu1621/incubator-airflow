# coding: utf-8

from xTool.exceptions import XToolException


def topological_sort(tasks):
    """拓扑排序
    Sorts tasks in topographical order, such that a task comes after any of its
    upstream dependencies.

    Heavily inspired by:
    http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

    :return: list of tasks in topological order
    """
    graph_sorted = []

    # special case
    if not tasks:
        return tuple(graph_sorted)

    # copy the the tasks so we leave it unmodified
    graph_unsorted = tasks[:]

    # Run until the unsorted graph is empty.
    while graph_unsorted:
        # Go through each of the node/edges pairs in the unsorted
        # graph. If a set of edges doesn't contain any nodes that
        # haven't been resolved, that is, that are still in the
        # unsorted graph, remove the pair from the unsorted graph,
        # and append it to the sorted graph. Note here that by using
        # using the items() method for iterating, a copy of the
        # unsorted graph is used, allowing us to modify the unsorted
        # graph as we move through it. We also keep a flag for
        # checking that that graph is acyclic, which is true if any
        # nodes are resolved during each pass through the graph. If
        # not, we need to bail out as the graph therefore can't be
        # sorted.
        acyclic = False
        for node in graph_unsorted:
            for edge in node.upstream_list:
                if edge in graph_unsorted:
                    break
            # no edges in upstream tasks
            else:
                # 无环
                acyclic = True
                graph_unsorted.remove(node)
                graph_sorted.append(node)

        if not acyclic:
            raise XToolException("A cyclic dependency occurred")

    return tuple(graph_sorted)
