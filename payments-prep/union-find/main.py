from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UnionFind:
    n: int
    _parent: list[int] = field(init=False, repr=False)
    _rank: list[int] = field(init=False, repr=False)


    def __post_init__(self) -> None:
        self._parent = list(range(self.n)) # each node is its own root
        self._rank = [0] * self.n

    def find(self, x: int) -> int:
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x]) # path compression
        return self._parent[x]

    def union(self, a: int, b: int) -> None:
        root_a, root_b = self.find(a), self.find(b)
        if root_a== root_b:
            return
        if self._rank[root_a] < self._rank[root_b]:
            self._parent[root_a] = root_b
        elif self._parent[root_a] > self._rank[root_b]:
            self._rank[root_b] = root_a
        else:
            self._parent[root_b] = root_a
            self._rank[root_a] += 1

    def connected(self, a: int, b: int) -> bool:
        return self.find(a) == self.find(b)