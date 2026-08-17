

"""
Graph
collection of points and lines connectiong some subset of them.
points are called vertices and lines are called edges.

3.1 Storing Graphs (Adjacency Matrix)
(Adjacency List)

"""
n = 5
matrix = [[0 for _ in range(n)] in range(n)]

class AdjGraph:
    vertices: int
    matrix: list[list[int]]

    def __init__(self, n: int):
        self.vertices = n
        self.matrix = [[0 for _ in range(n)] for _ in range(n)]

    def create_matrix(n: int) -> list[list[int]]:
        return [[0 for _ in range(n)] for _ in range(n)]

    def insert_edge(self, u: int, v: int, edge: int) -> None:
        self.matrix[u][v] = edge
        self.matrix[v][u] = edge

        for i in range(self.vertices):
            print(self.matrix[i])
            for j in range(self.vertices):
                print(self.matrix[i][j], end=" ")


    def get_edge(self, u: int, v: int) -> int:
        return self.matrix[u][v]



"""
weighted and unweighted graphs
"""

"""
Adjacency List
"""

class Node:
    def __init__(self, data: int):
        self.data = data
        self.next = None


class List:
    def __init__(self):
        self.head = None

    def insert(self, data: int) -> None:
        new_node = Node(data)
        new_node.next = self.head
        self.head = new_node

    def display(self) -> None:
        current = self.head
        while current:
            print(current.data, end=" ")
            current = current.next
        print()


class AdjListGraph:
    def __init__(self, vertices: int):
        self.vertices = vertices
        self.adj_list = [List() for _ in range(vertices)]

    def insert_edge(self, u: int, v: int) -> None:
        self.adj_list[u].insert(v)
        self.adj_list[v].insert(u)

    def display(self) -> None:
        for i in range(self.vertices):
            print(f"Vertex {i}: ", end="")
            self.adj_list[i].display()

    def is_cyclic(self, graph, v: int) -> bool:
        visited = [False] * v
        rec_stack = [False] * v

        for i in range(v):
            visited[i] = False
            rec_stack[i] = False

            for u in range(v):
                if not visited[u]:
                    if self._is_cyclic_util(u, visited, rec_stack):
                        return True

    def _is_cyclic_util(self, v: int, visited: list[bool], rec_stack: list[bool]) -> bool:
        visited[v] = True
        rec_stack[v] = True

        current = self.adj_list[v].head
        while current:
            if not visited[current.data]:
                if self._is_cyclic_util(current.data, visited, rec_stack):
                    return True
            elif rec_stack[current.data]:
                return True
            current = current.next

        rec_stack[v] = False
        return False


"""
Graph Traversal
"""

def dfs(node, graph, visited):
    if visited[node]:
        return
    visited[node] = True
    print(node, end=" ")
    for i in range(len(graph[node])):
        dfs(i, graph, visited)



def bfs_modified(G, source):
    Q = []
    distance = [float('inf')] * len(G)
    Q.append(source)
    distance[source] = 0
    while Q:
        u = Q.pop(0)
        for v in G.adj_list[u]:
            if distance[u] + cost[u][v] < distance[v]:
                distance[v] = distance[u] + cost[u][v]
                Q.append(v)

    return distance


class PriorityQueue:
    def __init__(self):
        self.elements = []

    def empty(self):
        return not self.elements

    def put(self, item, priority):
        heapq.heappush(self.elements, (priority, item))

    def get(self):
        return heapq.heappop(self.elements)[1]


def dijkstra(graph, source):
    pq = PriorityQueue()
    pq.put(source, 0)
    distances = {vertex: float('infinity') for vertex in graph}
    distances[source] = 0

    while not pq.empty():
        current_vertex = pq.get()

        for neighbor, weight in graph[current_vertex].items():
            distance = distances[current_vertex] + weight

            if distance < distances[neighbor]:
                distances[neighbor] = distance
                pq.put(neighbor, distance)

    return distances


class ShortestPath:
    def __init__(self, graph):
        self.graph = graph


    def _min_distance(self, distances, visited):
        min_distance = float('infinity')
        min_vertex = None

        for vertex in self.graph:
            if distances[vertex] < min_distance and not visited[vertex]:
                min_distance = distances[vertex]
                min_vertex = vertex

        return min_vertex

    def dijkstra(self, start):
        distances = {vertex: float('infinity') for vertex in self.graph}
        spt_set = {vertex: False for vertex in self.graph}

        distances[start] = 0
        for count in range(len(self.graph)):
            u = self._min_distance(distances, spt_set)
            spt_set[u] = True

            for neighbor, weight in self.graph[u].items():
                if not spt_set[neighbor] and distances[u] + weight < distances[neighbor]:
                    distances[neighbor] = distances[u] + weight

        return distances





"""
A* Pathfinding Algorithm

"""


manhattan_distance = lambda a, b: abs(a[0] - b[0]) + abs(a[1] - b[1])


class Job:
    def __init__(self, start_time, finish_time, profit, acc_prof):
        self.time = (start_time, finish_time)
        self.profit = profit
        self.acc_prof = acc_prof


d = Job(1, 3, 5, 5)
a = Job(2, 5, 6, 6)
f = Job(4, 6, 5, 10)
b = Job(6, 7, 4, 4)
e = Job(5, 8, 11, 11)
c = Job(7, 9, 2, 2)
jobs = [d, a, f, b, e, c]

def job_scheduling(jobs):
    n = len(jobs)
    jobs.sort(key=lambda x: x.time[1])
    Acc_Prof = [0] * n
    Prev = [-1] * n

    for i in range(n):
        Acc_Prof[i] = jobs[i].profit

        for j in range(i):
            if jobs[j].time[1] <= jobs[i].time[0]:
                if jobs[j].acc_prof + jobs[i].profit > jobs[i].acc_prof:
                    jobs[i].acc_prof = jobs[j].acc_prof + jobs[i].profit
                    Prev[i] = j

job_scheduling(jobs)


def finding_performed_jobs(job, acc_prof, max_profit):
    s = []
    for i in range(n, -1, -1) and max_profit > 0:
        if max_profit == acc_prof[i]:
            pass



def lcs(string1, string2, m, n):
    if m == 0 or n == 0:
        return 0
    if string1[m - 1] == string2[n - 1]:
        return 1 + lcs(string1, string2, m - 1, n - 1)
    else:
        return max(lcs(string1, string2, m, n - 1), lcs(string1, string2, m - 1, n))




"""
Kruskals MST
"""

def kruskal_mst(graph):
    parent = {}
    rank = {}

    def find(node):
        if parent[node] != node:
            parent[node] = find(parent[node])
        return parent[node]

    def union(node1, node2):
        root1 = find(node1)
        root2 = find(node2)

        if root1 != root2:
            if rank[root1] > rank[root2]:
                parent[root2] = root1
            else:
                parent[root1] = root2
                if rank[root1] == rank[root2]:
                    rank[root2] += 1

    for node in graph['vertices']:
        parent[node] = node
        rank[node] = 0

    mst_edges = []
    edges = sorted(graph['edges'], key=lambda x: x[2])

    for edge in edges:
        u, v, weight = edge
        if find(u) != find(v):
            union(u, v)
            mst_edges.append(edge)

    return mst_edges

