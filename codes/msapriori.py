from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional
from pathlib import Path
import time
import math

# ------------------------------
# Abstract classes (lightweight)
# ------------------------------
class AbstractItemset:
    def size(self) -> int:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError

    def getAbsoluteSupport(self) -> int:
        raise NotImplementedError

    def getRelativeSupport(self, nbObject: int) -> float:
        return float(self.getAbsoluteSupport()) / float(nbObject)

    def contains(self, item: int) -> bool:
        raise NotImplementedError


class AbstractOrderedItemset(AbstractItemset):
    def get(self, position: int) -> int:
        raise NotImplementedError

    def getLastItem(self) -> int:
        return self.get(self.size() - 1)

    def __str__(self) -> str:
        if self.size() == 0:
            return "EMPTYSET"
        return " ".join(str(self.get(i)) for i in range(self.size()))

    def contains(self, item: int) -> bool:
        # Items are assumed sorted by MIS order / lexical
        for i in range(self.size()):
            gi = self.get(i)
            if gi == item:
                return True
            if gi > item:
                return False
        return False

    # Helper comparisons for joins/equals are implemented directly in Algo


# ------------------------------
# Concrete Itemset
# ------------------------------
@dataclass
class Itemset(AbstractOrderedItemset):
    itemset: List[int]
    support: int = 0

    def __init__(self, items: Optional[List[int]] = None):
        self.itemset = list(items or [])
        self.support = 0

    # convenience ctor for single int or list/tuple of ints
    @classmethod
    def of(cls, items) -> "Itemset":
        if isinstance(items, int):
            return cls([items])
        return cls(list(items))

    def getItems(self) -> List[int]:
        return self.itemset

    def getAbsoluteSupport(self) -> int:
        return self.support

    def size(self) -> int:
        return len(self.itemset)

    def get(self, position: int) -> int:
        return self.itemset[position]

    def setAbsoluteSupport(self, support: int) -> None:
        self.support = support

    def increaseTransactionCount(self) -> None:
        self.support += 1

    def cloneItemSetMinusOneItem(self, itemToRemove: int) -> "Itemset":
        return Itemset([x for x in self.itemset if x != itemToRemove])

    def cloneItemSetMinusAnItemset(self, itemsetToNotKeep: "Itemset") -> "Itemset":
        forbid = set(itemsetToNotKeep.itemset)
        return Itemset([x for x in self.itemset if x not in forbid])

    def __str__(self) -> str:
        return " ".join(str(x) for x in self.itemset)


# ------------------------------
# Memory Logger (minimal)
# ------------------------------
class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self):
        self._max_memory_mb = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self._max_memory_mb = 0.0

    def checkMemory(self) -> float:
        # Lightweight placeholder; Python stdlib has no simple portable “used RAM” call.
        # Keeping API compatible with Java version.
        return self._max_memory_mb

    def getMaxMemory(self) -> float:
        return self._max_memory_mb


# ------------------------------
# MS-Apriori Algorithm
# ------------------------------
class AlgoMSApriori:
    def __init__(self):
        # These will be initialized during run
        self.k: int = 0
        self.MIS: List[int] = []
        self.LSRelative: int = 0
        self.database: List[List[int]] = []
        self.itemsetCount: int = 0
        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0
        self.maxItemsetSize: int = 2**31 - 1  # mimic Integer.MAX_VALUE
        self._writer_path: Optional[Path] = None

    # Comparator equivalent: sort by (MIS[item], item)
    def _mis_key(self, item: int) -> Tuple[int, int]:
        return (self.MIS[item], item)

    def setMaximumPatternLength(self, length: int) -> None:
        self.maxItemsetSize = length

    def runAlgorithm(self, input_path: str, output_path: str, beta: float, LS: float) -> None:
        self.startTimestamp = time.time()
        self.itemsetCount = 0
        MemoryLogger.getInstance().reset()
        self._writer_path = Path(output_path)

        # --------- First DB scan: read transactions, count item supports
        mapItemCount: dict[int, int] = {}
        self.database = []
        transactionCount = 0
        maxItemID = -1

        ipath = Path(input_path)
        with ipath.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line[0] in {"#", "%", "@"}:
                    continue
                items = [int(x) for x in line.split()]
                for it in items:
                    mapItemCount[it] = mapItemCount.get(it, 0) + 1
                    if it > maxItemID:
                        maxItemID = it
                self.database.append(items)
                transactionCount += 1

        if maxItemID < 0:
            # empty database
            self.endTimestamp = time.time()
            self._write_lines([])  # create/clear output file
            return

        # --------- Initialize MIS and LSRelative
        self.MIS = [0] * (maxItemID + 1)
        self.LSRelative = int(math.ceil(LS * transactionCount))

        # --------- k = 1, compute MIS for each item and output frequent 1-itemsets
        self.k = 1
        M: List[int] = []
        out_lines: List[str] = []
        for item, supp in mapItemCount.items():
            M.append(item)
            self.MIS[item] = max(int(beta * supp), self.LSRelative)
        # save frequent 1-itemsets
        if self.maxItemsetSize >= 1:
            for item, supp in mapItemCount.items():
                if supp >= self.MIS[item]:
                    out_lines.append(f"{item} #SUP: {supp}")
                    self.itemsetCount += 1

        # Sort items by MIS order (ascending MIS, then id)
        M.sort(key=self._mis_key)

        # If no frequent 1-itemsets, stop
        if self.itemsetCount == 0:
            self.endTimestamp = time.time()
            self._write_lines(out_lines)
            return

        # --------- Build F set as in paper
        F: List[int] = []
        minMIS: Optional[int] = None
        # find first item with support >= MIS[item]
        i = 0
        while i < len(M):
            it = M[i]
            if mapItemCount[it] >= self.MIS[it]:
                F.append(it)
                minMIS = self.MIS[it]
                break
            i += 1
        i += 1
        while i < len(M):
            it = M[i]
            if minMIS is not None and mapItemCount[it] >= minMIS:
                F.append(it)
            i += 1

        # --------- Sort each transaction by MIS order
        for t in self.database:
            t.sort(key=self._mis_key)

        # --------- k >= 2
        if self.maxItemsetSize > 1:
            level: Optional[List[Itemset]] = None
            self.k = 2
            while True:
                MemoryLogger.getInstance().checkMemory()

                if self.k == 2:
                    candidatesK = self._generateCandidate2(F)
                else:
                    candidatesK = self._generateCandidateSizeK(level or [])

                # Count supports (one database pass)
                for trans in self.database:
                    for cand in candidatesK:
                        # two-pointer containment check in MIS order
                        pos = 0
                        for item in trans:
                            if item == cand.get(pos):
                                pos += 1
                                if pos == cand.size():
                                    cand.increaseTransactionCount()
                                    break
                            elif self._mis_key(item) > self._mis_key(cand.get(pos)):
                                # due to total order, cannot be contained
                                break

                # Filter by MIS of the first item (since items sorted by MIS in cand)
                level = []
                for cand in candidatesK:
                    if cand.getAbsoluteSupport() >= self.MIS[cand.get(0)]:
                        level.append(cand)
                        out_lines.append(f"{str(cand)} #SUP: {cand.getAbsoluteSupport()}")
                        self.itemsetCount += 1

                self.k += 1
                if not level or self.k > self.maxItemsetSize:
                    break

        self.endTimestamp = time.time()
        MemoryLogger.getInstance().checkMemory()
        self._write_lines(out_lines)

    # ---------- Candidate generation (k=2)
    def _generateCandidate2(self, frequent1: List[int]) -> List[Itemset]:
        cands: List[Itemset] = []
        # frequent1 is already MIS-ordered
        for i in range(len(frequent1)):
            item1 = frequent1[i]
            for j in range(i + 1, len(frequent1)):
                item2 = frequent1[j]
                cands.append(Itemset.of([item1, item2]))
        return cands

    # ---------- Candidate generation (k>2)
    def _generateCandidateSizeK(self, levelK_1: List[Itemset]) -> List[Itemset]:
        # levelK_1 contains frequent itemsets of size k-1, items are MIS-ordered
        candidates: List[Itemset] = []

        # For faster subset checks later, we'll keep a set of tuples
        # (The original Java used a binary search over a MIS-ordered list.)
        level_set = {tuple(x.getItems()) for x in levelK_1}

        for i in range(len(levelK_1)):
            itemset1 = levelK_1[i].getItems()
            for j in range(i + 1, len(levelK_1)):
                itemset2 = levelK_1[j].getItems()

                # Compare positions
                ok_join = True
                for p in range(len(itemset1)):
                    if p == len(itemset1) - 1:
                        # last element of itemset1 must be <= last element of itemset2 by MIS order,
                        # and strictly smaller (different)
                        if self._mis_key(itemset1[p]) > self._mis_key(itemset2[p]):
                            ok_join = False
                            break
                        if itemset1[p] == itemset2[p]:
                            ok_join = False
                            break
                    else:
                        if itemset1[p] != itemset2[p]:
                            # due to MIS order, decide which loop to continue
                            if self._mis_key(itemset1[p]) < self._mis_key(itemset2[p]):
                                ok_join = False  # continue inner loop
                                # emulate "continue loop2" by just skipping this pair
                                break
                            else:
                                ok_join = False
                                # emulate "continue loop1" by breaking to outer check
                                break
                if not ok_join:
                    continue

                new_items = list(itemset1) + [itemset2[-1]]

                if self._allSubsetsFrequent(new_items, level_set):
                    candidates.append(Itemset.of(new_items))

        return candidates

    def _allSubsetsFrequent(self, c: List[int], level_set: set[Tuple[int, ...]]) -> bool:
        # Optimization in the paper (and Java): if removing first item when MIS(c0)!=MIS(c1), skip check.
        for posRemoved in range(len(c)):
            if posRemoved == 0 and self.MIS[c[0]] != self.MIS[c[1]]:
                continue
            subset = tuple(c[k] for k in range(len(c)) if k != posRemoved)
            if subset not in level_set:
                return False
        return True

    # ---------- Output helpers
    def _write_lines(self, lines: List[str]) -> None:
        outp = self._writer_path or Path("output_python.txt")
        with outp.open("w", encoding="utf-8") as w:
            for line in lines:
                w.write(line + "\n")

    # ---------- Stats (console)
    def printStats(self) -> None:
        print("=============  MSAPRIORI - STATS =============")
        print(f" The algorithm stopped at level {self.k - 1}, because there is no candidate")
        print(f" Frequent itemsets count : {self.itemsetCount}")
        print(f" Maximum memory usage : {MemoryLogger.getInstance().getMaxMemory()} mb")
        print(f" Total time ~ {int((self.endTimestamp - self.startTimestamp)*1000)} ms")
        print("===================================================")


# ------------------------------
# Main (same defaults as Java)
# ------------------------------
def main():
    root = Path(__file__).resolve().parent
    input_file = root / "contextIGB.txt"
    output_file = root / "output_python.txt"

    beta = 1.0
    LS = 0.4

    algo = AlgoMSApriori()
    # Uncomment to limit pattern length:
    # algo.setMaximumPatternLength(3)

    algo.runAlgorithm(str(input_file), str(output_file), beta, LS)
    algo.printStats()
    print(f"\nInput : {input_file}")
    print(f"Output: {output_file}")

if __name__ == "__main__":
    main()
