#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
import os
import sys
import time
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# MemoryLogger
# ---------------------------------------------------------------------------

class MemoryLogger:
    """
    Equivalent to MemoryLogger.java (singleton-like API).

    In Java, MemoryLogger records actual JVM memory usage.
    Here, for portability and because it does not affect pattern results,
    we keep a simple stub that tracks a constant 0.0 MB.
    """

    _instance: Optional["MemoryLogger"] = None

    def __init__(self) -> None:
        self._max_memory_mb: float = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self._max_memory_mb = 0.0

    def checkMemory(self) -> float:
        # We do not inspect real process memory (no external libraries).
        # This stub keeps the API shape; value is always 0.0 MB.
        return self._max_memory_mb

    def getMaxMemory(self) -> float:
        return self._max_memory_mb


# ---------------------------------------------------------------------------
# Itemset + Itemsets
# ---------------------------------------------------------------------------

class Itemset:
    """
    Equivalent to Itemset.java

    Represents an itemset (array of ints) with a support count.
    """

    def __init__(self, items: List[int] | None = None) -> None:
        self.itemset: List[int] = items if items is not None else []
        self.support: int = 0

    def getItems(self) -> List[int]:
        return self.itemset

    def size(self) -> int:
        return len(self.itemset)

    def get(self, position: int) -> int:
        return self.itemset[position]

    def getAbsoluteSupport(self) -> int:
        return self.support

    def setAbsoluteSupport(self, support: int) -> None:
        self.support = support

    def increaseTransactionCount(self) -> None:
        self.support += 1

    def cloneItemSetMinusOneItem(self, item_to_remove: int) -> "Itemset":
        """Equivalent logic to cloneItemSetMinusOneItem(Integer itemToRemove)"""
        new_items = [x for x in self.itemset if x != item_to_remove]
        return Itemset(new_items)

    def __str__(self) -> str:
        if not self.itemset:
            return "EMPTYSET"
        return " ".join(str(x) for x in self.itemset)

    def print(self) -> None:
        # For compatibility with Java's itemset.print()
        print(str(self), end="")


class Itemsets:
    """
    Equivalent to Itemsets.java

    Stores itemsets grouped by size (level).
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.levels: List[List[Itemset]] = []
        self.levels.append([])  # Level 0 (empty)
        self.itemsetsCount: int = 0

    def addItemset(self, itemset: Itemset, k: int) -> None:
        while len(self.levels) <= k:
            self.levels.append([])
        self.levels[k].append(itemset)
        self.itemsetsCount += 1

    def getLevels(self) -> List[List[Itemset]]:
        return self.levels

    def getItemsetsCount(self) -> int:
        return self.itemsetsCount

    def setName(self, new_name: str) -> None:
        self.name = new_name

    def decreaseItemsetCount(self) -> None:
        self.itemsetsCount -= 1

    def printItemsets(self, nbObject: int) -> None:
        """
        Equivalent to printItemsets(int nbObject) in Itemsets.java.
        """
        print(f" ------- {self.name} -------")
        pattern_count = 0
        level_count = 0
        for level in self.levels:
            print(f"  L{level_count} ")
            for itemset in level:
                print(f"  pattern {pattern_count}:  ", end="")
                itemset.print()
                print(f"support :  {itemset.getAbsoluteSupport()}")
                pattern_count += 1
            level_count += 1
        print(" --------------------------------")


# ---------------------------------------------------------------------------
# MISNode + MISTree
# ---------------------------------------------------------------------------

class MISNode:
    """
    Equivalent to MISNode.java

    A node in the MIS-based FP-tree used by CFPGrowth++.
    """

    def __init__(self) -> None:
        self.itemID: int = -1
        self.counter: int = 1
        self.parent: Optional["MISNode"] = None
        self.childs: List["MISNode"] = []
        self.nodeLink: Optional["MISNode"] = None

    def getChildWithID(self, id_: int) -> Optional["MISNode"]:
        for child in self.childs:
            if child.itemID == id_:
                return child
        return None

    def getChildIndexWithID(self, id_: int) -> int:
        for idx, child in enumerate(self.childs):
            if child.itemID == id_:
                return idx
        return -1


class SortKey:
    """
    Helper class to mimic Java's Comparator<Integer> behavior in Python's sort.
    """

    def __init__(self, value: int, comparator):
        self.value = value
        self._cmp = comparator

    def __lt__(self, other: "SortKey") -> bool:
        return self._cmp(self.value, other.value) < 0


class MISTree:
    """
    Equivalent to MISTree.java
    """

    def __init__(self) -> None:
        self.mapItemNodes: Dict[int, MISNode] = {}
        self.mapItemLastNode: Dict[int, MISNode] = {}
        self.headerList: List[int] = []
        self.root: MISNode = MISNode()

    def addTransaction(self, transaction: List[int]) -> None:
        currentNode = self.root
        for item in transaction:
            child = currentNode.getChildWithID(item)
            if child is None:
                newNode = MISNode()
                newNode.itemID = item
                newNode.parent = currentNode
                currentNode.childs.append(newNode)
                currentNode = newNode
                self._fixNodeLinks(item, newNode)
            else:
                child.counter += 1
                currentNode = child

    def addPrefixPath(
        self,
        prefixPath: List[MISNode],
        mapSupportBeta: Dict[int, int],
        minMIS: int,
    ) -> None:
        pathCount = prefixPath[0].counter
        currentNode = self.root

        for i in range(len(prefixPath) - 1, 0, -1):
            pathItem = prefixPath[i]
            if mapSupportBeta.get(pathItem.itemID, 0) < minMIS:
                continue

            child = currentNode.getChildWithID(pathItem.itemID)
            if child is None:
                newNode = MISNode()
                newNode.itemID = pathItem.itemID
                newNode.parent = currentNode
                newNode.counter = pathCount
                currentNode.childs.append(newNode)
                currentNode = newNode
                self._fixNodeLinks(pathItem.itemID, newNode)
            else:
                child.counter += pathCount
                currentNode = child

    def _fixNodeLinks(self, item: int, newNode: MISNode) -> None:
        lastNode = self.mapItemLastNode.get(item)
        if lastNode is not None:
            lastNode.nodeLink = newNode
        self.mapItemLastNode[item] = newNode

        headernode = self.mapItemNodes.get(item)
        if headernode is None:
            self.mapItemNodes[item] = newNode

    def createHeaderList(self, itemComparator) -> None:
        self.headerList = list(self.mapItemNodes.keys())
        self.headerList.sort(key=lambda x: SortKey(x, itemComparator))

    def deleteFromHeaderList(self, item: int, itemComparator) -> None:
        if item in self.headerList:
            self.headerList.remove(item)

    def MISPruning(self, item: int) -> None:
        headernode = self.mapItemNodes.get(item)
        while headernode is not None:
            if not headernode.childs:
                headernode.parent.childs.remove(headernode)
            else:
                headernode.parent.childs.remove(headernode)
                headernode.parent.childs.extend(headernode.childs)
                for node in headernode.childs:
                    node.parent = headernode.parent
            headernode = headernode.nodeLink

    def MISMerge(self, treeRoot: Optional[MISNode]) -> None:
        if treeRoot is None:
            return

        i = 0
        while i < len(treeRoot.childs):
            node1 = treeRoot.childs[i]
            j = i + 1
            while j < len(treeRoot.childs):
                node2 = treeRoot.childs[j]
                if node2.itemID == node1.itemID:
                    node1.counter += node2.counter
                    node1.childs.extend(node2.childs)
                    treeRoot.childs.pop(j)

                    headernode = self.mapItemNodes.get(node1.itemID)
                    if headernode is node2:
                        self.mapItemNodes[node2.itemID] = node2.nodeLink
                    else:
                        while headernode is not None and headernode.nodeLink is not node2:
                            headernode = headernode.nodeLink
                        if headernode is not None:
                            headernode.nodeLink = headernode.nodeLink.nodeLink
                else:
                    j += 1
            i += 1

        for node1 in list(treeRoot.childs):
            self.MISMerge(node1)

    def print(self, root: MISNode) -> None:
        if root.itemID != -1:
            print(root.itemID, end="")
        print(" ", end="")
        for node in root.childs:
            self.print(node)


# ---------------------------------------------------------------------------
# AlgoCFPGrowthPP
# ---------------------------------------------------------------------------

class AlgoCFPGrowthPP:
    """
    Equivalent to AlgoCFPGrowth.java
    """

    def __init__(self) -> None:
        self.startTimestamp: float = 0.0
        self.endTime: float = 0.0
        self.transactionCount: int = 0
        self.itemsetCount: int = 0

        self.writer = None
        self.patterns: Optional[Itemsets] = None

        self.MIS: List[int] = []
        self.minMIS: int = 0

        self.memoryLogger: MemoryLogger = MemoryLogger.getInstance()

        def cmp(o1: int, o2: int) -> int:
            compare = self.MIS[o2] - self.MIS[o1]
            if compare == 0:
                return o1 - o2
            return compare

        self.itemComparator = cmp

    def _initMISfromFile(self, mis_path: str) -> None:
        self.minMIS = sys.maxsize
        maxItemID = 0
        mapMIS: Dict[int, int] = {}

        with open(mis_path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                parts = line.split()
                item = int(parts[0])
                itemMIS = int(parts[1])

                if itemMIS != 0 and itemMIS < self.minMIS:
                    self.minMIS = itemMIS
                mapMIS[item] = itemMIS
                if item > maxItemID:
                    maxItemID = item

        self.MIS = [0] * (maxItemID + 1)
        for item, mis_val in mapMIS.items():
            self.MIS[item] = mis_val

    def _initMISfromFrequency(
        self,
        input_path: str,
        mapSupport: Dict[int, int],
        beta: float,
        LS: float,
    ) -> int:
        maxItemID = 0
        self.transactionCount = 0

        with open(input_path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                for token in line.split():
                    item = int(token)
                    mapSupport[item] = mapSupport.get(item, 0) + 1
                    if item > maxItemID:
                        maxItemID = item
                self.transactionCount += 1

        self.MIS = [0] * (maxItemID + 1)
        self.minMIS = 1
        LSRelative = math.ceil(LS * self.transactionCount)

        for item, sup in mapSupport.items():
            mis_val = int(beta * sup)
            if mis_val < LSRelative:
                mis_val = LSRelative
            self.MIS[item] = mis_val
            if mis_val < self.minMIS:
                self.minMIS = mis_val

        return self.minMIS

    def runAlgorithm(
        self,
        input_path: str,
        output_path: Optional[str],
        mis_path: str,
    ) -> Optional[Itemsets]:
        self.startTimestamp = time.time()
        self.memoryLogger.reset()
        self.memoryLogger.checkMemory()
        self.transactionCount = 0
        self.itemsetCount = 0

        self._initMISfromFile(mis_path)

        if output_path is None:
            self.writer = None
            self.patterns = Itemsets("FREQUENT ITEMSETS")
        else:
            self.patterns = None
            out_dir = os.path.dirname(output_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            self.writer = open(output_path, "w", encoding="utf-8")

        mapSupport: Dict[int, int] = {}
        tree = MISTree()

        with open(input_path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                parts = line.split()
                transaction: List[int] = []
                for token in parts:
                    item = int(token)
                    mapSupport[item] = mapSupport.get(item, 0) + 1
                    transaction.append(item)
                self.transactionCount += 1
                transaction.sort(key=lambda x: SortKey(x, self.itemComparator))
                tree.addTransaction(transaction)

        tree.createHeaderList(self.itemComparator)

        sw = False
        for item, sup in list(mapSupport.items()):
            if sup < self.minMIS:
                tree.deleteFromHeaderList(item, self.itemComparator)
                tree.MISPruning(item)
                sw = True
        if sw:
            tree.MISMerge(tree.root)

        if self.transactionCount > 0:
            minsupRelative = self.minMIS / self.transactionCount
        else:
            minsupRelative = 0.0
        print(
            f"[DEBUG] minsupRelative={minsupRelative:.6f}, "
            f"minMIS={self.minMIS}, transactions={self.transactionCount}"
        )

        prefixAlpha: List[int] = []
        if len(tree.headerList) > 0:
            self._cfpgrowth(tree, prefixAlpha, self.transactionCount, mapSupport)

        self.memoryLogger.checkMemory()

        if self.writer is not None:
            self.writer.close()
            self.writer = None

        self.endTime = time.time()
        return self.patterns

    def _cfpgrowth(
        self,
        tree: MISTree,
        prefixAlpha: List[int],
        prefixSupport: int,
        mapSupport: Dict[int, int],
    ) -> None:
        if len(tree.headerList) == 1:
            item = tree.headerList[0]
            node = tree.mapItemNodes[item]
            if node.nodeLink is None:
                if prefixAlpha and node.counter >= self.MIS[prefixAlpha[0]]:
                    self._writeItemset(prefixAlpha, node.itemID, node.counter)
            else:
                self._cfpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport)
        else:
            self._cfpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport)

    def _cfpgrowthMoreThanOnePath(
        self,
        tree: MISTree,
        prefixAlpha: List[int],
        prefixSupport: int,
        mapSupport: Dict[int, int],
    ) -> None:
        for idx in range(len(tree.headerList) - 1, -1, -1):
            item = tree.headerList[idx]
            support = mapSupport[item]

            mis = self.MIS[item] if not prefixAlpha else self.MIS[prefixAlpha[0]]
            if support < mis:
                continue

            betaSupport = prefixSupport if prefixSupport < support else support

            if support >= mis:
                self._writeItemset(prefixAlpha, item, betaSupport)

            prefixPaths: List[List[MISNode]] = []
            path = tree.mapItemNodes[item]
            while path is not None:
                if path.parent.itemID != -1:
                    prefixPath: List[MISNode] = []
                    prefixPath.append(path)
                    parent = path.parent
                    while parent.itemID != -1:
                        prefixPath.append(parent)
                        parent = parent.parent
                    prefixPaths.append(prefixPath)
                path = path.nodeLink

            mapSupportBeta: Dict[int, int] = {}
            for prefixPath in prefixPaths:
                pathCount = prefixPath[0].counter
                for j in range(1, len(prefixPath)):
                    node = prefixPath[j]
                    mapSupportBeta[node.itemID] = mapSupportBeta.get(node.itemID, 0) + pathCount

            treeBeta = MISTree()
            for prefixPath in prefixPaths:
                treeBeta.addPrefixPath(prefixPath, mapSupportBeta, self.minMIS)
            treeBeta.createHeaderList(self.itemComparator)

            if treeBeta.root.childs:
                beta = prefixAlpha + [item]
                self._cfpgrowth(treeBeta, beta, betaSupport, mapSupportBeta)

    def _writeItemset(
        self,
        prefix: List[int],
        lastItem: int,
        support: int,
    ) -> None:
        self.itemsetCount += 1
        full = prefix + [lastItem]

        if self.writer is not None:
            line = "{} #SUP: {}\n".format(" ".join(str(x) for x in full), support)
            self.writer.write(line)
        else:
            items_sorted = sorted(full)
            itemset_obj = Itemset(items_sorted)
            itemset_obj.setAbsoluteSupport(support)
            if self.patterns is not None:
                self.patterns.addItemset(itemset_obj, itemset_obj.size())

    def printStats(self) -> None:
        print("=============  CFP-GROWTH++ - STATS =============")
        print(f" Transactions count from database : {self.transactionCount}")
        print(f" Max memory usage: {self.memoryLogger.getMaxMemory()} mb")
        print(f" Frequent itemsets count : {self.itemsetCount}")
        total_ms = (self.endTime - self.startTimestamp) * 1000.0
        print(f" Total time {total_ms:.0f} ms")
        print("===================================================")

    def getDatabaseSize(self) -> int:
        return self.transactionCount


# ---------------------------------------------------------------------------
# Direct run (read files directly from the script folder)
# ---------------------------------------------------------------------------

def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(script_dir, "contextCFPGrowth.txt")
    mis_path = os.path.join(script_dir, "MIS.txt")
    output_path = os.path.join(script_dir, "cfpgrowth++_outputs.txt")

    algo = AlgoCFPGrowthPP()
    t0 = time.perf_counter()
    algo.runAlgorithm(input_path, output_path, mis_path)
    t1 = time.perf_counter()

    algo.printStats()
    print()
    print(f"Total elapsed wall time: {t1 - t0:.3f}s")
    print()
    print(f"Mining complete! Output saved to: {output_path}")
    print("=========================================")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)