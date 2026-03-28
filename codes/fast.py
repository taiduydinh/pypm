from __future__ import annotations

import math
import time
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional


class AlgoFast:
    """Python translation of the FAST sequential pattern mining algorithm."""

    def __init__(self) -> None:
        self.ds: Optional[FastDataset] = None
        self.sequence_tree: Optional[SequenceTree] = None
        self.start_timestamp: int = 0
        self.end_timestamp: int = 0
        self.pattern_count: int = 0
        self.max_sup: float = float("inf")

    def run(self) -> None:
        self.itemset_extension()
        MemoryLogger.get_instance().check_memory()
        self.sequence_tree = self.sequence_extension()

    def get_frequent_sequences(self) -> List["SequenceNode"]:
        if self.sequence_tree is None:
            return []
        return SequenceTree.visit(self.sequence_tree)

    def itemset_extension(self) -> None:
        tree = ItemsetTree()
        root = tree.get_root()

        queue: deque[ItemsetNode] = deque()
        for pos, (key, sil) in enumerate(self.ds.frequent_itemsets_items() if self.ds else []):
            node = tree.add_child(root, Itemset(*key.split(" ")), sil, pos)
            queue.append(node)

        while queue:
            node = queue.popleft()
            self._itemset_extension(tree, node)
            queue.extend(node.get_children())

    def _itemset_extension(self, tree: "ItemsetTree", node: "ItemsetNode") -> None:
        children = node.get_parent().get_children()
        pos = 0

        for i in range(node.get_position() + 1, len(children)):
            right_brother = children[i]
            sil = SparseIdList.IStep(node.get_sil(), right_brother.get_sil())

            if self.ds and self.ds.get_abs_min_sup() <= sil.get_absolute_support() <= self.ds.get_abs_max_sup():
                new_itemset = node.get_itemset().clone()
                new_itemset.add_item(right_brother.get_itemset().get_last())
                if self.ds:
                    self.ds.item_sil_map[new_itemset.concatenate()] = sil
                tree.add_child(node, new_itemset, sil, pos)
                pos += 1

    def sequence_extension(self) -> "SequenceTree":
        if self.ds is None:
            return SequenceTree(0)

        sequence_tree = SequenceTree(self.ds.get_num_rows())
        queue: deque[SequenceNode] = deque()

        for key, sil in self.ds.frequent_itemsets_items():
            sequence = Sequence(Itemset(*key.split(" ")))
            vil = sil.get_starting_vil()
            node = sequence_tree.add_child(sequence_tree.get_root(), sequence, vil, sil.get_absolute_support())
            queue.append(node)

        while queue:
            node = queue.popleft()
            self._sequence_extension(sequence_tree, node)
            queue.extend(node.get_children())
        return sequence_tree

    def _sequence_extension(self, tree: "SequenceTree", node: "SequenceNode") -> None:
        if self.ds is None:
            return

        vil_node = node.get_vertical_id_list()
        brothers = node.get_parent().get_children()

        for brother_node in brothers:
            count = 0
            new_pos_list: List[Optional[ListNode]] = [None] * len(vil_node.get_elements())
            vil_brother = brother_node.get_vertical_id_list()

            for i in range(len(vil_node.get_elements())):
                list_node = vil_node.get_elements()[i]
                list_node_brother = vil_brother.get_elements()[i] if vil_brother else None

                if list_node is None or list_node_brother is None:
                    continue

                if list_node.get_column() < list_node_brother.get_column():
                    new_pos_list[i] = list_node_brother
                    count += 1
                else:
                    while list_node_brother is not None and list_node.get_column() >= list_node_brother.get_column():
                        list_node_brother = list_node_brother.next()
                    if list_node_brother is not None:
                        new_pos_list[i] = list_node_brother
                        count += 1

            if self.ds.get_abs_min_sup() <= count <= self.ds.get_abs_max_sup():
                sequence = node.get_sequence().clone()
                sequence.add(brother_node.get_sequence().get_last_itemset())
                tree.add_child(node, sequence, VerticalIdList(new_pos_list, count), count)

    def _write_patterns(self, output_file: Path) -> None:
        nodes = self.get_frequent_sequences()
        lines = [str(node) for node in nodes]
        output_file.write_text("\n".join(lines), encoding="utf-8")
        self.pattern_count = len(nodes)

    def run_algorithm(self, input_file: str, output_path: str, minsup: float) -> None:
        self.start_timestamp = int(time.time() * 1000)
        MemoryLogger.get_instance().reset()

        self.ds = FastDataset.from_prefixspan_source(input_file, minsup, self.max_sup)
        self.run()
        self._write_patterns(Path(output_path))

        MemoryLogger.get_instance().check_memory()
        self.end_timestamp = int(time.time() * 1000)

    def print_statistics(self) -> None:
        total_time = (self.end_timestamp - self.start_timestamp) / 1000.0
        stats = [
            "=============  Algorithm Fast - STATISTICS =============",
            f"Pattern count : {self.pattern_count}",
            f"Total time: {total_time} s",
            f"Max memory (mb) : {MemoryLogger.get_instance().get_max_memory()}",
            "===================================================",
        ]
        print("\n".join(stats))

    def set_maximum_support(self, max_sup: float) -> None:
        self.max_sup = max_sup


class ClosedSequenceNode:
    def __init__(self, parent: Optional["ClosedSequenceNode"], sequence: "Sequence", vil: "VerticalIdList", absolute_support: int) -> None:
        self.vil = vil
        self.children: List[ClosedSequenceNode] = []
        self.parent = parent
        self.sequence = sequence
        self.type = "toCheck"
        self.absolute_support = absolute_support

    def get_children(self) -> List["ClosedSequenceNode"]:
        return self.children

    def get_parent(self) -> Optional["ClosedSequenceNode"]:
        return self.parent

    def get_vertical_id_list(self) -> "VerticalIdList":
        return self.vil

    def get_sequence(self) -> "Sequence":
        return self.sequence

    def get_type(self) -> str:
        return self.type

    def set_type(self, node_type: str) -> None:
        self.type = node_type

    def get_absolute_support(self) -> int:
        return self.absolute_support

    def __str__(self) -> str:
        return f"{self.sequence} #SUP: {self.absolute_support}"

    def contains_last_itemset(self, n: "ClosedSequenceNode") -> bool:
        if self.sequence.get_last_itemset() == n.sequence.get_last_itemset():
            return False
        return self.sequence.get_last_itemset().contains(n.get_sequence().get_last_itemset())


class FastDataset:
    ITEMSET_SEPARATOR = "-1"
    SEQUENCE_SEPARATOR = "-2"

    def __init__(self, num_rows: int, min_sup: float, max_sup: float = 1.0) -> None:
        self.item_sil_map: Dict[str, SparseIdList] = {}
        self.num_rows = num_rows
        self.min_sup = min_sup
        self.max_sup = max_sup
        self.abs_min_sup = self.absolute_support(min_sup, num_rows)
        if self.abs_min_sup == 0:
            self.abs_min_sup = 1
        if math.isinf(max_sup):
            self.abs_max_sup = int(num_rows)
        else:
            self.abs_max_sup = self.absolute_support(max_sup, num_rows)
            if self.abs_max_sup == 0:
                self.abs_max_sup = 1

    def compute_frequent_items(self) -> None:
        new_map: Dict[str, SparseIdList] = {}
        for item, sparse_id_list in self.item_sil_map.items():
            if self.abs_min_sup <= sparse_id_list.get_absolute_support() <= self.abs_max_sup:
                new_map[item] = sparse_id_list
        self.item_sil_map = {k: new_map[k] for k in sorted(new_map.keys())}

    def get_frequent_itemsets(self) -> Dict[str, "SparseIdList"]:
        return self.item_sil_map

    def frequent_itemsets_items(self) -> List[tuple[str, "SparseIdList"]]:
        return [(k, self.item_sil_map[k]) for k in sorted(self.item_sil_map.keys())]

    def get_sparse_id_list(self, item: str) -> Optional["SparseIdList"]:
        return self.item_sil_map.get(item)

    def get_num_rows(self) -> int:
        return self.num_rows

    def get_abs_min_sup(self) -> int:
        return self.abs_min_sup

    def get_abs_max_sup(self) -> int:
        return self.abs_max_sup

    @staticmethod
    def from_prefixspan_source(path: str, relative_min_support: float, relative_max_support: float) -> "FastDataset":
        file_path = Path(path)
        num_rows = 0
        for line in file_path.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or line.startswith("%") or line.startswith("@"):  # comments/metadata
                continue
            num_rows += 1

        dataset = FastDataset(num_rows, relative_min_support, relative_max_support)

        line_number = 0
        with file_path.open("r", encoding="utf-8") as in_file:
            for raw_line in in_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or line.startswith("%") or line.startswith("@"):
                    continue

                trans_id = 1
                for token in line.split():
                    if token == FastDataset.ITEMSET_SEPARATOR:
                        trans_id += 1
                        continue
                    if token == FastDataset.SEQUENCE_SEPARATOR:
                        break

                    sil = dataset.item_sil_map.get(token)
                    if sil is None:
                        sil = SparseIdList(int(num_rows))
                        dataset.item_sil_map[token] = sil
                    sil.add_element(line_number, trans_id)
                line_number += 1

        dataset.compute_frequent_items()
        return dataset

    @staticmethod
    def absolute_support(relative_support: float, total_count: int) -> int:
        if math.isinf(relative_support):
            return int(total_count)
        return int(math.ceil(relative_support * total_count))


class Itemset:
    def __init__(self, *items: str) -> None:
        self.elements: List[str] = list(items)

    def add_item(self, *items: str) -> None:
        self.elements.extend(items)

    def clone(self) -> "Itemset":
        return Itemset(*self.elements)

    def contains(self, other: "Itemset") -> bool:
        return all(elem in self.elements for elem in other)

    def concatenate(self) -> str:
        return " ".join(self.elements).strip()

    def __iter__(self) -> Iterable[str]:
        return iter(self.elements)

    def __len__(self) -> int:
        return len(self.elements)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Itemset) and self.elements == other.elements

    def __hash__(self) -> int:
        return hash(tuple(self.elements))

    def __str__(self) -> str:
        return self.concatenate()

    def get_last(self) -> str:
        return self.elements[-1]


class ItemsetNode:
    def __init__(self, itemset: Optional[Itemset] = None, parent: Optional["ItemsetNode"] = None, sil: Optional["SparseIdList"] = None, position: int = -1) -> None:
        self.position = position
        self.children: List[ItemsetNode] = []
        self.parent = parent
        self.itemset = itemset
        self.sil = sil

    def get_children(self) -> List["ItemsetNode"]:
        return self.children

    def get_parent(self) -> "ItemsetNode":
        return self.parent

    def get_position(self) -> int:
        return self.position

    def get_itemset(self) -> Itemset:
        return self.itemset

    def get_sil(self) -> "SparseIdList":
        return self.sil

    def __str__(self) -> str:
        return str(self.itemset)


class ItemsetTree:
    def __init__(self) -> None:
        self.root = ItemsetNode()

    def add_child(self, parent: ItemsetNode, itemset: Itemset, sil: "SparseIdList", position: int) -> ItemsetNode:
        new_node = ItemsetNode(itemset, parent, sil, position)
        parent.get_children().append(new_node)
        return new_node

    def get_root(self) -> ItemsetNode:
        return self.root


class ListNode:
    def __init__(self, column: int, next_node: Optional["ListNode"] = None) -> None:
        self.column = column
        self._next = next_node

    def get_column(self) -> int:
        return self.column

    def set_next(self, node: Optional["ListNode"]) -> None:
        self._next = node

    def next(self) -> Optional["ListNode"]:
        return self._next

    def before(self, succ: Optional["ListNode"]) -> Optional["ListNode"]:
        while succ is not None:
            if self.column < succ.column:
                return succ
            succ = succ.next()
        return None

    def equal(self, succ: Optional["ListNode"]) -> Optional["ListNode"]:
        while succ is not None:
            if self.column == succ.column:
                return succ
            succ = succ.next()
        return None

    def __repr__(self) -> str:
        return f"[ : {self.column}]"


class MainTestFast:
    @staticmethod
    def file_to_path(filename: str) -> str:
        base = Path(__file__).resolve().parent
        return str(base / filename)

    @staticmethod
    def main() -> None:
        input_file = MainTestFast.file_to_path("contextPrefixSpan.txt")
        # write Python output next to this script (src directory)
        output_path = Path(__file__).resolve().parent / "output_python.txt"

        algorithm = AlgoFast()
        minsup = 0.5

        algorithm.run_algorithm(input_file, str(output_path), minsup)
        algorithm.print_statistics()


class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self) -> None:
        self.max_memory: float = 0.0

    @classmethod
    def get_instance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_max_memory(self) -> float:
        return self.max_memory

    def reset(self) -> None:
        self.max_memory = 0.0

    def check_memory(self) -> float:
        current_memory = 0.0
        try:
            import os
            import psutil

            process = psutil.Process(os.getpid())
            current_memory = process.memory_info().rss / 1024 / 1024
        except Exception:
            current_memory = 0.0

        if current_memory > self.max_memory:
            self.max_memory = current_memory
        return current_memory


class Sequence:
    def __init__(self, *itemsets: Itemset) -> None:
        self.elements: List[Itemset] = list(itemsets)

    def add(self, element: Itemset) -> None:
        self.elements.append(element)

    def get_last_item(self) -> str:
        return self.get_last_itemset().get_last()

    def __str__(self) -> str:
        buff = []
        for itemset in self.elements:
            buff.append(itemset.concatenate())
            buff.append(" -1 ")
        return "".join(buff) + "-2"

    def get_last_itemset(self) -> Itemset:
        return self.elements[-1]

    def length(self) -> int:
        return len(self.elements)

    def clone(self) -> "Sequence":
        return Sequence(*[itemset.clone() for itemset in self.elements])

    def __iter__(self) -> Iterable[Itemset]:
        return iter(self.elements)

    def get_elements(self) -> List[Itemset]:
        return self.elements

    def contains_itemset(self, itemset: Itemset) -> bool:
        return any(i.contains(itemset) for i in self.elements)

    def contains(self, other: "Sequence") -> bool:
        if len(self.elements) < len(other.elements):
            return False

        match_index = 0
        for itemset in other:
            next_index = -1
            for i in range(match_index, len(self.elements)):
                if self.elements[i].contains(itemset):
                    next_index = i
                    break
            if next_index == -1:
                return False
            match_index = next_index + 1
        return True

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Sequence) and self.elements == other.elements

    def __hash__(self) -> int:
        return hash(tuple(self.elements))


class SequenceNode:
    def __init__(self, vertical_id_list: Optional["VerticalIdList"], sequence: Sequence, parent: Optional["SequenceNode"], abs_support: int) -> None:
        self.vertical_id_list = vertical_id_list
        self.children: List[SequenceNode] = []
        self.parent = parent
        self.sequence = sequence
        self.abs_support = abs_support

    def get_abs_support(self) -> int:
        return self.abs_support

    def get_children(self) -> List["SequenceNode"]:
        return self.children

    def get_parent(self) -> Optional["SequenceNode"]:
        return self.parent

    def get_vertical_id_list(self) -> "VerticalIdList":
        return self.vertical_id_list

    def get_sequence(self) -> Sequence:
        return self.sequence

    def __str__(self) -> str:
        return f"{self.sequence} #SUP: {self.get_abs_support()}"


class SequenceTree:
    def __init__(self, num_sequences: int) -> None:
        self.root = SequenceNode(None, Sequence(), None, int(num_sequences))

    def add_child(self, parent: SequenceNode, sequence: Sequence, vil: "VerticalIdList", absolute_support: int) -> SequenceNode:
        new_node = SequenceNode(vil, sequence, parent, absolute_support)
        parent.get_children().append(new_node)
        return new_node

    def get_root(self) -> SequenceNode:
        return self.root

    @staticmethod
    def visit(tree: "SequenceTree") -> List[SequenceNode]:
        queue: deque[SequenceNode] = deque()
        result: List[SequenceNode] = []
        queue.extend(tree.get_root().get_children())

        while queue:
            current_node = queue.popleft()
            result.append(current_node)
            queue.extend(current_node.get_children())
        return result


class SparseIdList:
    def __init__(self, rows: int) -> None:
        self.vector: List[Optional[SparseIdList.TransactionIds]] = [None] * rows
        self.absolute_support = 0

    def length(self) -> int:
        return len(self.vector)

    def add_element(self, row: int, value: int) -> None:
        if self.vector[row] is None:
            self.vector[row] = SparseIdList.TransactionIds()
            self.absolute_support += 1
        self.vector[row].add(ListNode(value))

    def get_element(self, row: int, col: int) -> Optional[ListNode]:
        if self.vector[row] is not None and col < len(self.vector[row]):
            return self.vector[row][col]
        return None

    @staticmethod
    def IStep(a: "SparseIdList", b: "SparseIdList") -> "SparseIdList":
        sparse_id_list = SparseIdList(a.length())
        for i in range(a.length()):
            a_node = a.get_element(i, 0)
            b_node = b.get_element(i, 0)

            while a_node is not None and b_node is not None:
                if a_node.get_column() == b_node.get_column():
                    sparse_id_list.add_element(i, b_node.get_column())
                    a_node = a_node.next()
                    b_node = b_node.next()
                elif a_node.get_column() > b_node.get_column():
                    b_node = b_node.next()
                else:
                    a_node = a_node.next()
        return sparse_id_list

    def get_starting_vil(self) -> "VerticalIdList":
        vil_elements: List[Optional[ListNode]] = [None] * self.length()
        for i in range(self.length()):
            vil_elements[i] = self.get_element(i, 0)
        return VerticalIdList(vil_elements, self.absolute_support)

    def get_absolute_support(self) -> int:
        return self.absolute_support

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SparseIdList):
            return False

        for i in range(len(self.vector)):
            these = self.vector[i]
            those = other.vector[i]
            if these is None and those is None:
                continue
            if these is None or those is None:
                return False
            if len(these) != len(those):
                return False
            for j in range(len(these)):
                if these[j].get_column() != those[j].get_column():
                    return False
        return True

    def __hash__(self) -> int:
        return hash(tuple(tuple(t.get_column() for t in lst) if lst else None for lst in self.vector))

    def __repr__(self) -> str:
        lines = []
        for curr_list in self.vector:
            if curr_list is not None:
                lines.append(" ".join(str(node) for node in curr_list))
            else:
                lines.append("null")
        return "\n".join(lines)

    class TransactionIds(list):
        def add(self, node: ListNode) -> None:
            if self:
                self[-1].set_next(node)
            self.append(node)

        def __repr__(self) -> str:
            return "".join(str(elem) for elem in self)


class VerticalIdList:
    def __init__(self, elements: List[Optional[ListNode]], absolute_support: int) -> None:
        self.elements = elements
        self.absolute_support = absolute_support

    def get_elements(self) -> List[Optional[ListNode]]:
        return self.elements


if __name__ == "__main__":
    MainTestFast.main()
