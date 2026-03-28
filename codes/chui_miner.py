#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations

import os
import time
from typing import Dict, List, Optional, Set, Tuple


# =========================
# MemoryLogger (SPMF style)
# =========================
class MemoryLogger:
    _instance = None

    def __init__(self) -> None:
        self.max_memory_mb: float = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self.max_memory_mb = 0.0

    def checkMemory(self) -> float:
        # Cross-platform "good enough" approximation without psutil:
        # We'll just keep 0.0 (so code runs everywhere).
        # If you want accurate memory, install psutil and update this.
        current = 0.0
        if current > self.max_memory_mb:
            self.max_memory_mb = current
        return current

    def getMaxMemory(self) -> float:
        return self.max_memory_mb


# =========================
# Data structures
# =========================
class Element:
    __slots__ = ("tid", "iutils", "rutils")

    def __init__(self, tid: int, iutils: int, rutils: int) -> None:
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class UtilityList:
    __slots__ = ("item", "sumIutils", "sumRutils", "elements")

    def __init__(self, item: int) -> None:
        self.item: int = item
        self.sumIutils: int = 0
        self.sumRutils: int = 0
        self.elements: List[Element] = []

    def addElement(self, element: Element) -> None:
        self.sumIutils += element.iutils
        self.sumRutils += element.rutils
        self.elements.append(element)

    def getSupport(self) -> int:
        return len(self.elements)

    def getUtils(self) -> int:
        return self.sumIutils


class PairItemUtility:
    __slots__ = ("item", "utility")

    def __init__(self, item: int = 0, utility: int = 0) -> None:
        self.item = item
        self.utility = utility

    def __repr__(self) -> str:
        return f"[{self.item},{self.utility}]"


class Itemset:
    __slots__ = ("itemset", "utility", "support")

    def __init__(self, items: List[int] | Tuple[int, ...] | None = None, utility: int = 0, support: int = 0) -> None:
        self.itemset = list(items) if items is not None else []
        self.utility = utility
        self.support = support

    def getItems(self) -> List[int]:
        return self.itemset

    def getUtility(self) -> int:
        return self.utility

    def size(self) -> int:
        return len(self.itemset)

    def __repr__(self) -> str:
        return " ".join(str(x) for x in self.itemset)


# =========================
# AlgoCHUIMiner
# =========================
class AlgoCHUIMiner:
    def __init__(self, useEUCPstrategy: bool = True) -> None:
        self.useEUCPstrategy = useEUCPstrategy

        self.startTimestamp: int = 0
        self.endTimestamp: int = 0
        self.chuidCount: int = 0
        self.candidateCount: int = 0

        self.mapItemToTWU: Dict[int, int] = {}
        self.writer = None
        self.minUtility: int = 0

        # EUCP structure (FMAP): itemX -> { itemY : TWU({x,y}) }
        self.mapFMAP: Dict[int, Dict[int, int]] = {}

        # Save-to-memory structures (only if output is None)
        self.listItemsetsBySize: Optional[List[List[Itemset]]] = None
        self.setOfItemsInClosedItemsets: Optional[Set[int]] = None

    # ------------------------
    # Utilities
    # ------------------------
    def compareItems(self, item1: int, item2: int) -> int:
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else compare

    @staticmethod
    def appendItem(itemset: List[int], item: int) -> List[int]:
        return itemset + [item]

    @staticmethod
    def findElementWithTID(ulist: UtilityList, tid: int) -> Optional[Element]:
        # Binary search on tid (Java assumes elements are added in increasing tid, so it's sorted)
        lst = ulist.elements
        first = 0
        last = len(lst) - 1
        while first <= last:
            middle = (first + last) >> 1
            mtid = lst[middle].tid
            if mtid < tid:
                first = middle + 1
            elif mtid > tid:
                last = middle - 1
            else:
                return lst[middle]
        return None

    def isPassingHUIPruning(self, ulist: UtilityList) -> bool:
        return (ulist.sumIutils + ulist.sumRutils) >= self.minUtility

    def containsAllTIDS(self, ul1: UtilityList, ul2: UtilityList) -> bool:
        # Check if ul1 contains all tids of ul2
        for ex in ul2.elements:
            if self.findElementWithTID(ul1, ex.tid) is None:
                return False
        return True

    def checkEUCPStrategy(self, itemX: int, itemY: int) -> bool:
        # prune if TWU({x,y}) < minUtil or not present
        mp = self.mapFMAP.get(itemX)
        if mp is not None:
            twuF = mp.get(itemY)
            if twuF is None or twuF < self.minUtility:
                return True
        return False

    def construct(self, uX: UtilityList, uE: UtilityList) -> UtilityList:
        # construct utility list for Xe (as in provided Java)
        uXE = UtilityList(uE.item)
        for elmX in uX.elements:
            elmE = self.findElementWithTID(uE, elmX.tid)
            if elmE is None:
                continue
            # tricky part: rutils - elmE.iutils
            elmXe = Element(elmX.tid, elmX.iutils + elmE.iutils, elmX.rutils - elmE.iutils)
            uXE.addElement(elmXe)
        return uXE

    # ------------------------
    # Output
    # ------------------------
    def saveToMemory(self, itemset: List[int], sumIutils: int, support: int) -> None:
        assert self.listItemsetsBySize is not None
        assert self.setOfItemsInClosedItemsets is not None

        while len(self.listItemsetsBySize) <= len(itemset):
            self.listItemsetsBySize.append([])

        self.listItemsetsBySize[len(itemset)].append(Itemset(itemset, sumIutils, support))
        for it in itemset:
            self.setOfItemsInClosedItemsets.add(it)

    def saveCHUI(self, itemset: List[int], sumIutils: int, support: int) -> None:
        self.chuidCount += 1
        if self.writer is None:
            self.saveToMemory(itemset, sumIutils, support)
        else:
            # match Java format
            buf = []
            for x in itemset:
                buf.append(str(x))
            line = " ".join(buf) + "  #SUP: " + str(support) + " #UTIL: " + str(sumIutils)
            self.writer.write(line + "\n")

    # ------------------------
    # Dup check
    # ------------------------
    def is_dup(self, newgenTIDs: UtilityList, preset: List[UtilityList]) -> bool:
        for j in preset:
            containsAll = True
            for elmX in newgenTIDs.elements:
                if self.findElementWithTID(j, elmX.tid) is None:
                    containsAll = False
                    break
            if containsAll:
                # Java comment: original paper wrote false but should be true
                return True
        return False

    # ------------------------
    # Main recursive procedure
    # ------------------------
    def chuimineClosed_eucp(
        self,
        firstTime: bool,
        closedSet: List[int],
        closedSetUL: Optional[UtilityList],
        preset: List[UtilityList],
        postset: List[UtilityList],
    ) -> None:
        for iUL in postset:
            # L4 newgen_TIDs
            if firstTime:
                newgen_TIDs = iUL
            else:
                assert closedSetUL is not None
                newgen_TIDs = self.construct(closedSetUL, iUL)

            # HUI pruning
            if not self.isPassingHUIPruning(newgen_TIDs):
                continue

            # newGen = closedSet U {i}
            newGen = self.appendItem(closedSet, iUL.item)

            # if not duplicate
            if not self.is_dup(newgen_TIDs, preset):
                closedSetNew = newGen
                closedsetNewTIDs = newgen_TIDs
                postsetNew: List[UtilityList] = []

                passedHUIPruning = True
                for jUL in postset:
                    if jUL.item == iUL.item or self.compareItems(jUL.item, iUL.item) < 0:
                        continue

                    # EUCP
                    if self.useEUCPstrategy and self.checkEUCPStrategy(iUL.item, jUL.item):
                        continue
                    self.candidateCount += 1

                    if self.containsAllTIDS(jUL, newgen_TIDs):
                        closedSetNew = self.appendItem(closedSetNew, jUL.item)
                        closedsetNewTIDs = self.construct(closedsetNewTIDs, jUL)
                        if not self.isPassingHUIPruning(closedsetNewTIDs):
                            passedHUIPruning = False
                            break
                    else:
                        postsetNew.append(jUL)

                if passedHUIPruning:
                    if closedsetNewTIDs.sumIutils >= self.minUtility:
                        self.saveCHUI(closedSetNew, closedsetNewTIDs.sumIutils, len(closedsetNewTIDs.elements))

                    presetNew = list(preset)  # copy
                    self.chuimineClosed_eucp(False, closedSetNew, closedsetNewTIDs, presetNew, postsetNew)

                preset.append(iUL)

    # ------------------------
    # Run algorithm
    # ------------------------
    def runAlgorithm(self, input_path: str, minUtility: int, output_path: Optional[str]) -> Optional[List[List[Itemset]]]:
        MemoryLogger.getInstance().reset()
        self.minUtility = minUtility

        if output_path is not None:
            self.writer = open(output_path, "w", encoding="utf-8")
        else:
            self.writer = None
            self.listItemsetsBySize = []
            self.setOfItemsInClosedItemsets = set()

        if self.useEUCPstrategy:
            self.mapFMAP = {}

        self.startTimestamp = int(time.time() * 1000)

        # PASS 1: TWU per item
        self.mapItemToTWU = {}
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                split = line.split(":")
                items = split[0].split()
                tu = int(split[1])
                for it in items:
                    item = int(it)
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + tu

        # Create UtilityLists for promising items
        listOfUtilityLists: List[UtilityList] = []
        mapItemToUtilityList: Dict[int, UtilityList] = {}

        for item, twu in self.mapItemToTWU.items():
            if twu >= self.minUtility:
                ul = UtilityList(item)
                mapItemToUtilityList[item] = ul
                listOfUtilityLists.append(ul)

        # Sort by TWU ascending (and lexicographic if tie)
        listOfUtilityLists.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        # PASS 2: build utility lists + build FMAP (EUCP)
        tid = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                split = line.split(":")
                items = split[0].split()
                utils = split[2].split()

                revised: List[PairItemUtility] = []
                newTU = 0

                for i in range(len(items)):
                    item = int(items[i])
                    util = int(utils[i])
                    if self.mapItemToTWU.get(item, 0) >= self.minUtility:
                        revised.append(PairItemUtility(item, util))
                        newTU += util  # NEW OPTIMIZATION (as in Java)

                # sort by global item order (TWU asc)
                revised.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                remainingUtility = newTU
                for i in range(len(revised)):
                    pair = revised[i]
                    remainingUtility -= pair.utility

                    ul = mapItemToUtilityList[pair.item]
                    ul.addElement(Element(tid, pair.utility, remainingUtility))

                    # EUCP updates
                    if self.useEUCPstrategy:
                        mp = self.mapFMAP.get(pair.item)
                        if mp is None:
                            mp = {}
                            self.mapFMAP[pair.item] = mp
                        for j in range(i + 1, len(revised)):
                            after = revised[j]
                            mp[after.item] = mp.get(after.item, 0) + newTU

                tid += 1

        MemoryLogger.getInstance().checkMemory()

        # Mine recursively
        self.chuimineClosed_eucp(True, [], None, [], listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        if self.writer is not None:
            self.writer.close()
            self.writer = None

        self.endTimestamp = int(time.time() * 1000)
        return self.listItemsetsBySize

    def printStats(self) -> None:
        print("=============  CHUIMiner ALGORITHM SPMF 0.97e - STATS =============")
        print(f" Total time ~ {self.endTimestamp - self.startTimestamp} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" Closed High-utility itemsets count : {self.chuidCount}")
        print(f" Candidate count : {self.candidateCount}")
        print("=====================================================")


# =========================
# Main (like MainTestCHUIMiner_saveToFile.java)
# =========================
def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))

    # Same as Java MainTest
    INPUT = os.path.join(here, "DB_Utility.txt")
    MIN_UTILITY = 24
    OUTPUT = os.path.join(here, "output_py.txt")

    if not os.path.exists(INPUT):
        raise FileNotFoundError(
            f"Cannot find input file: {INPUT}\n"
            "Put DB_Utility.txt in the same folder as chui_miner.py (or edit INPUT path)."
        )

    algo = AlgoCHUIMiner(useEUCPstrategy=True)
    algo.runAlgorithm(INPUT, MIN_UTILITY, OUTPUT)
    algo.printStats()
    print(f"\nSaved output to: {OUTPUT}")


if __name__ == "__main__":
    main()
