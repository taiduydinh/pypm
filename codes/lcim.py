# lcim.py
# Python port of SPMF AlgoLCIM + CostList (LCIM - Cost Efficient Itemset Mining)

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import math
import time
import os
import psutil


# -----------------------------
# Memory logger (simple)
# -----------------------------
class MemoryLogger:
    _instance = None

    def __init__(self) -> None:
        self._max_mb = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self._max_mb = 0.0

    def checkMemory(self) -> None:
        process = psutil.Process(os.getpid())
        mb = process.memory_info().rss / (1024 * 1024)
        if mb > self._max_mb:
            self._max_mb = mb

    def getMaxMemory(self) -> float:
        return self._max_mb


# -----------------------------
# CostList (exact port)
# -----------------------------
class CostList:
    # Static buffer (shared) used in Java to reduce allocations
    _costBuffer: List[int] = []

    def __init__(self, item: int) -> None:
        self.item: int = item
        self.utility: int = 0
        self.cost: int = 0
        self.tids: List[int] = []
        self.costs: List[int] = []
        self.lowerbound: float = -1.0  # cached

    def addElement(self, tid: int, utility: int, cost: int) -> None:
        self.utility += utility
        self.cost += cost
        self.tids.append(tid)
        self.costs.append(cost)

    def getSupport(self) -> int:
        return len(self.tids)

    def getUtility(self) -> int:
        return self.utility

    def getAverageUtility(self) -> float:
        return self.utility / float(len(self.tids))

    def getAverageCost(self) -> float:
        return self.cost / float(len(self.tids))

    def getCostLowerBound(self, minsup: int) -> float:
        # Java: if (lowerbound == -1) { ... }
        if self.lowerbound == -1.0:
            CostList._costBuffer.clear()
            CostList._costBuffer.extend(self.costs)
            # sort descending
            CostList._costBuffer.sort(reverse=True)
            lb = 0.0
            limit = minsup if minsup < len(CostList._costBuffer) else len(CostList._costBuffer)
            for i in range(limit):
                lb += CostList._costBuffer[i]
            lb = lb / float(len(self.tids))
            self.lowerbound = lb
        return self.lowerbound

    def __str__(self) -> str:
        s = f"[item:{self.item} cost:{self.cost} utility:{self.utility} tids: {self.tids}"
        if self.tids:
            s += f" support: {self.getSupport()} avgcost: {self.getAverageCost()} avgutility: {self.getAverageUtility()}"
        s += "]"
        return s


# -----------------------------
# AlgoLCIM (port)
# -----------------------------
class AlgoLCIM:
    # Different total orders (same names as Java)
    class OrderType:
        ascendingSupport = "ascendingSupport"
        descendingSupport = "descendingSupport"
        lexicographicalOrder = "lexicographicalOrder"
        ascendingCost = "ascendingCost"
        descendingCost = "descendingCost"
        ascendingLowerBound = "ascendingLowerBound"
        descendingLowerBound = "descendingLowerBound"

    def __init__(self) -> None:
        # stats
        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0
        self.patternCount: int = 0
        self.candidateCount: int = 0
        self.constructedListCount: int = 0

        # options (match Java defaults)
        self.TOTAL_ORDER = self.OrderType.ascendingSupport
        self.COST_PRUNING = True
        self.MATRIX_SUPPORT_PRUNING = True
        self.PRINT_MATRIX_SUPPORT_SIZE = True
        self.ENABLE_LA_PRUNE = True
        self.DEBUG = False

        self.BUFFERS_SIZE = 200
        self.itemsetBuffer: List[int] = [0] * self.BUFFERS_SIZE

        # minsup
        self.minsupRelative: int = 0

        # maps
        self.mapItemToCost: Optional[Dict[int, int]] = None
        self.mapItemToLowerBound: Optional[Dict[int, float]] = None
        self.mapItemToSupport: Dict[int, int] = {}

        # db info
        self.transactionsToUtility: List[int] = []

        # matrix support pruning
        self.mapMatrixSupport: Optional[Dict[int, Dict[int, int]]] = None

        self.writer = None  # file handle

    def runAlgorithm(self, input_file: str, output_file: str, minUtility: float, maxcost: float, minsupp: float) -> None:
        mem = MemoryLogger.getInstance()
        mem.reset()

        self.itemsetBuffer = [0] * self.BUFFERS_SIZE
        self.patternCount = 0
        self.candidateCount = 0
        self.constructedListCount = 0

        self.startTimestamp = time.time()
        self.writer = open(output_file, "w", encoding="utf-8")

        self.mapItemToSupport = {}

        # maps needed for ordering
        if self.TOTAL_ORDER in (self.OrderType.ascendingCost, self.OrderType.descendingCost):
            self.mapItemToCost = {}
        else:
            self.mapItemToCost = None

        if self.TOTAL_ORDER in (self.OrderType.ascendingLowerBound, self.OrderType.descendingLowerBound):
            self.mapItemToLowerBound = {}
        else:
            self.mapItemToLowerBound = None

        if self.MATRIX_SUPPORT_PRUNING:
            self.mapMatrixSupport = {}
        else:
            self.mapMatrixSupport = None

        # -------- PASS 1: count support of each item + db size
        databaseSize = 0
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                databaseSize += 1
                split = line.split(":")
                items = split[0].split()
                for it in items:
                    item = int(it)
                    self.mapItemToSupport[item] = self.mapItemToSupport.get(item, 0) + 1

        # relative minsup
        self.minsupRelative = int(math.ceil(minsupp * databaseSize))

        if self.DEBUG:
            print("======== PARAMETERS =========")
            print(" MINUTILITY:", minUtility)
            print(" MAXCOST:", maxcost)
            print(" MINSUP:", self.minsupRelative)

        self.transactionsToUtility = [0] * databaseSize

        # Create costlist for each frequent item
        mapItemToCostList: Dict[int, CostList] = {}
        for item, sup in self.mapItemToSupport.items():
            if sup >= self.minsupRelative:
                mapItemToCostList[item] = CostList(item)

        # -------- PASS 2: build costlists + matrix
        tid = 0
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = split[0].split()
                costValues = split[2].split()

                self.transactionsToUtility[tid] = int(split[1])

                revised: List[Tuple[int, int]] = []
                for i in range(len(items)):
                    item = int(items[i])
                    sup = self.mapItemToSupport.get(item, 0)
                    if sup >= self.minsupRelative:
                        revised.append((item, int(costValues[i])))

                # Java: sorting commented out => we do NOT sort revised

                for i, (item_i, cost_i) in enumerate(revised):
                    cl = mapItemToCostList[item_i]
                    cl.addElement(tid, self.transactionsToUtility[tid], cost_i)

                    # Matrix support pruning
                    if self.MATRIX_SUPPORT_PRUNING and self.mapMatrixSupport is not None:
                        fm = self.mapMatrixSupport.get(item_i)
                        if fm is None:
                            fm = {}
                            self.mapMatrixSupport[item_i] = fm

                        for j, (item_j, _) in enumerate(revised):
                            if j != i:
                                fm[item_j] = fm.get(item_j, 0) + 1

                tid += 1

        mem.checkMemory()

        # Filter promising costlists
        promising: List[CostList] = []
        for item, cl in mapItemToCostList.items():
            self.constructedListCount += 1
            self.candidateCount += 1

            if cl.getSupport() >= self.minsupRelative:
                if (not self.COST_PRUNING) or (cl.getCostLowerBound(self.minsupRelative) <= maxcost):
                    promising.append(cl)

                    if self.mapItemToCost is not None:
                        self.mapItemToCost[item] = cl.cost
                    if self.mapItemToLowerBound is not None:
                        self.mapItemToLowerBound[item] = cl.getCostLowerBound(self.minsupRelative)

        # Sort by total order
        promising.sort(key=lambda cl: self._sort_key(cl.item))

        if self.DEBUG:
            print("======== START SEARCH =========")

        # Mine recursively
        self._search(prefixLength=0, ULs=promising, minUtility=minUtility, maxcost=maxcost)

        mem.checkMemory()
        self.writer.close()
        self.endTimestamp = time.time()

    def _sort_key(self, item: int):
        if self.TOTAL_ORDER == self.OrderType.ascendingSupport:
            return (self.mapItemToSupport[item], item)
        elif self.TOTAL_ORDER == self.OrderType.descendingSupport:
            return (-self.mapItemToSupport[item], item)
        elif self.TOTAL_ORDER == self.OrderType.ascendingCost:
            assert self.mapItemToCost is not None
            return (self.mapItemToCost[item], item)
        elif self.TOTAL_ORDER == self.OrderType.descendingCost:
            assert self.mapItemToCost is not None
            return (-self.mapItemToCost[item], item)
        elif self.TOTAL_ORDER == self.OrderType.ascendingLowerBound:
            assert self.mapItemToLowerBound is not None
            return (self.mapItemToLowerBound[item], item)
        elif self.TOTAL_ORDER == self.OrderType.descendingLowerBound:
            assert self.mapItemToLowerBound is not None
            return (-self.mapItemToLowerBound[item], item)
        else:
            return (item,)

    def _search(self, prefixLength: int, ULs: List[CostList], minUtility: float, maxcost: float) -> None:
        for i in range(len(ULs)):
            X = ULs[i]

            # output if cost efficient
            if X.getAverageUtility() >= minUtility and X.getAverageCost() <= maxcost:
                self._writeOut(prefixLength, X)

            exULs: List[CostList] = []
            for j in range(i + 1, len(ULs)):
                Y = ULs[j]

                # Matrix support pruning
                if self.MATRIX_SUPPORT_PRUNING and self.mapMatrixSupport is not None:
                    fm = self.mapMatrixSupport.get(X.item)
                    if fm is not None:
                        sup_xy = fm.get(Y.item, 0)
                        if sup_xy < self.minsupRelative:
                            continue

                self.candidateCount += 1
                temp = self._construct(X, Y)
                if temp is not None and temp.getSupport() >= self.minsupRelative:
                    if (not self.COST_PRUNING) or (temp.getCostLowerBound(self.minsupRelative) <= maxcost):
                        exULs.append(temp)

            self.itemsetBuffer[prefixLength] = X.item
            self._search(prefixLength + 1, exULs, minUtility, maxcost)

        MemoryLogger.getInstance().checkMemory()

    def _construct(self, px: CostList, py: CostList) -> Optional[CostList]:
        xy = CostList(py.item)

        # LA-prune support bound
        if self.ENABLE_LA_PRUNE:
            totalSupport = min(len(px.tids), len(py.tids))
        else:
            totalSupport = 0

        py_tids = py.tids  # sorted by tid because tid increases during pass-2

        for idx in range(len(px.tids)):
            tid = px.tids[idx]
            costX = px.costs[idx]

            posY = self._find_tid(py_tids, tid)
            if posY == -1:
                if self.ENABLE_LA_PRUNE:
                    totalSupport -= 1
                    if totalSupport < self.minsupRelative:
                        return None
                continue

            costY = py.costs[posY]
            tu = self.transactionsToUtility[tid]
            xy.addElement(tid, tu, costX + costY)

        self.constructedListCount += 1
        return xy

    @staticmethod
    def _find_tid(sorted_list: List[int], tid: int) -> int:
        lo, hi = 0, len(sorted_list) - 1
        while lo <= hi:
            mid = (lo + hi) >> 1
            v = sorted_list[mid]
            if v < tid:
                lo = mid + 1
            elif v > tid:
                hi = mid - 1
            else:
                return mid
        return -1

    def _writeOut(self, prefixLength: int, costList: CostList) -> None:
        self.patternCount += 1

        parts = []
        for i in range(prefixLength):
            parts.append(str(self.itemsetBuffer[i]))
        parts.append(str(costList.item))

        s = " ".join(parts)
        s += " #AUTIL: " + str(costList.getAverageUtility())
        s += " #ACOST: " + str(costList.getAverageCost())
        s += " #SUP: " + str(costList.getSupport())

        self.writer.write(s + "\n")

        if self.DEBUG:
            print("WRITEOUT:", s)

    def printStats(self) -> None:
        ms = int((self.endTimestamp - self.startTimestamp) * 1000)
        print("=============  LCIM ALGORITHM v2.50 - STATS =============")
        print(f" Total time ~ {ms} ms")
        print(f" Max memory ~ {MemoryLogger.getInstance().getMaxMemory():.3f} MB")
        print(f" Itemset count: {self.patternCount}")
        print(f" Candidate count: {self.candidateCount}")
        print(f" Fully constructed cost-list count: {self.constructedListCount}")
        print("===================================================")


# -----------------------------
# MainTestLCIM equivalent
# -----------------------------
def main():

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    input_file = os.path.join(BASE_DIR, "DB_cost.txt")
    output_file = os.path.join(BASE_DIR, "output_py.txt")

    minutil = 10.0
    maxcost = 10.0
    minsup = 0.3

    algo = AlgoLCIM()
    algo.runAlgorithm(input_file, output_file, minutil, maxcost, minsup)
    algo.printStats()
    print("Done. Wrote:", output_file)


if __name__ == "__main__":
    main()