#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import argparse
import time
import sys
from typing import Dict, List, Optional, Tuple, Set


# ----------------------------------------------------------------------
# MemoryLogger (SPMF-like)
# ----------------------------------------------------------------------
class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self) -> None:
        self._max_memory_mb: float = 0.0

    @staticmethod
    def getInstance() -> "MemoryLogger":
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def getMaxMemory(self) -> float:
        return self._max_memory_mb

    def reset(self) -> None:
        self._max_memory_mb = 0.0

    def checkMemory(self) -> float:
        # Best-effort cross-platform memory measure.
        mem_mb = 0.0
        try:
            import resource  # Unix (macOS/Linux)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            rss = float(usage.ru_maxrss)
            if sys.platform == "darwin":
                mem_mb = rss / (1024.0 * 1024.0)  # bytes -> MB (macOS)
            else:
                mem_mb = rss / 1024.0  # KB -> MB (Linux)
        except Exception:
            mem_mb = self._max_memory_mb

        if mem_mb > self._max_memory_mb:
            self._max_memory_mb = mem_mb
        return mem_mb


# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------
@dataclass
class Element:
    tid: int
    iutils: int
    rutils: int


class UtilityList:
    def __init__(self, item: int) -> None:
        self.item: int = int(item)
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


@dataclass
class Itemset:
    items: Tuple[int, ...]
    utility: int
    support: int

    def __str__(self) -> str:
        return f"{list(self.items)} utility : {self.utility} support:  {self.support}"


@dataclass
class PairItemUtility:
    item: int = 0
    utility: int = 0

    def __str__(self) -> str:
        return f"[{self.item},{self.utility}]"


# ----------------------------------------------------------------------
# AlgoCLS_miner (Python port)
# ----------------------------------------------------------------------
class AlgoCLS_miner:
    def __init__(self, useChain_EUCP: bool, useCoverage: bool, useLBP: bool, usePreCheck: bool) -> None:
        self.useChain_EUCP = bool(useChain_EUCP)
        self.useCoverage = bool(useCoverage)
        self.useLBP = bool(useLBP)
        self.usePreCheck = bool(usePreCheck)

        self.startTimestamp: int = 0
        self.endTimestamp: int = 0

        self.chuidCount: int = 0
        self.candidateCount: int = 0

        self.minUtility: int = 0
        self.count1: int = 0
        self.count2: int = 0

        self.mapItemToTWU: Dict[int, int] = {}

        self.mapFMAP: Dict[int, Dict[int, int]] = {}
        self.Cov: Dict[int, List[int]] = {}

        self.listItemsetsBySize: Optional[List[List[Itemset]]] = None
        self.setOfItemsInClosedItemsets: Optional[Set[int]] = None

        self._writer = None  # file handle

    def runAlgorithm(self, input_path: str, minUtility: int, output_path: Optional[str]) -> Optional[List[List[Itemset]]]:
        MemoryLogger.getInstance().reset()

        self.minUtility = int(minUtility)
        self.chuidCount = 0
        self.candidateCount = 0
        self.count1 = 0
        self.count2 = 0

        if output_path is not None:
            outp = Path(output_path)
            outp.parent.mkdir(parents=True, exist_ok=True)
            self._writer = open(outp, "w", encoding="utf-8", newline="\n")
        else:
            self.listItemsetsBySize = []
            self.setOfItemsInClosedItemsets = set()

        if self.useChain_EUCP:
            self.mapFMAP = {}
        if self.useCoverage:
            self.Cov = {}

        self.startTimestamp = int(time.time() * 1000)

        # 1) First DB scan: compute TWU per item
        self.mapItemToTWU = {}
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                parts = line.split(":")
                items_str = parts[0].strip().split()
                tu = int(parts[1].strip())
                for it_s in items_str:
                    it = int(it_s)
                    self.mapItemToTWU[it] = self.mapItemToTWU.get(it, 0) + tu

        # 2) Create UtilityList objects for items with TWU >= minUtility
        listOfUtilityLists: List[UtilityList] = []
        mapItemToUtilityList: Dict[int, UtilityList] = {}

        for item, twu in self.mapItemToTWU.items():
            if twu >= self.minUtility:
                ul = UtilityList(item)
                mapItemToUtilityList[item] = ul
                listOfUtilityLists.append(ul)

        # Sort by TWU asc, then lexical (same as Java compareItems)
        listOfUtilityLists.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        # 3) Second DB scan: build utility lists + EUCP map
        tid = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                tid += 1
                parts = line.split(":")
                items = parts[0].strip().split()
                utils = parts[2].strip().split()

                newTU = 0
                revised: List[PairItemUtility] = []
                for i in range(len(items)):
                    pair = PairItemUtility(int(items[i]), int(utils[i]))
                    if self.mapItemToTWU.get(pair.item, 0) >= self.minUtility:
                        revised.append(pair)
                        newTU += pair.utility

                revised.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                remainingUtility = newTU
                for i in range(len(revised)):
                    pair = revised[i]
                    remainingUtility -= pair.utility

                    ul_item = mapItemToUtilityList[pair.item]
                    ul_item.addElement(Element(tid, pair.utility, remainingUtility))

                    if self.useChain_EUCP:
                        mapItem = self.mapFMAP.get(pair.item)
                        if mapItem is None:
                            mapItem = {}
                            self.mapFMAP[pair.item] = mapItem
                        for j in range(i + 1, len(revised)):
                            after = revised[j]
                            prev = mapItem.get(after.item)
                            if prev is None:
                                mapItem[after.item] = newTU
                            else:
                                mapItem[after.item] = prev + newTU

        # 4) Coverage construction
        if self.useCoverage:
            self._CoverageConstructProcedure(listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        # 5) Recursive mining
        self._CLS_Miner(True, [], None, [], listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        if self._writer is not None:
            self._writer.close()
            self._writer = None

        self.endTimestamp = int(time.time() * 1000)
        return self.listItemsetsBySize

    def _CLS_Miner(
        self,
        firstTime: bool,
        closedSet: List[int],
        closedSetUL: Optional[UtilityList],
        preset: List[UtilityList],
        postset: List[UtilityList],
    ) -> None:
        for iUL in postset:
            if firstTime:
                newgen_TIDs = iUL
            else:
                assert closedSetUL is not None
                newgen_TIDs = self._construct(closedSetUL, iUL)

            if self._isPassingHUIPruning(newgen_TIDs):
                newGen = self._appendItem(closedSet, iUL.item)

                if not self._improved_is_dup(newgen_TIDs, preset):
                    closedSetNew = newGen
                    closedsetNewTIDs = newgen_TIDs
                    postsetNew: List[UtilityList] = []

                    passedHUIPruning = True
                    for jUL in postset:
                        if jUL.item == iUL.item or self._compareItems(jUL.item, iUL.item) < 0:
                            continue

                        if self.useLBP and self._calculate_Con(newgen_TIDs, jUL) < self.minUtility:
                            continue

                        if self.useChain_EUCP:
                            shouldpassEUCS = False
                            for it in closedSetNew:
                                shouldpassEUCS = self._checkGenEUCPStrategy(it, jUL.item)
                                if shouldpassEUCS:
                                    break
                            if shouldpassEUCS:
                                self.count1 += 1
                                continue

                        if self.usePreCheck or self.useCoverage:
                            cond_cov = self.useCoverage and self._ifBelongToCov(iUL.item, jUL.item)
                            cond_pre = self.usePreCheck and (
                                self._preCheckContain(jUL, newgen_TIDs) and self._containsAllTIDS(jUL, newgen_TIDs)
                            )
                            if cond_cov or cond_pre:
                                closedSetNew = self._appendItem(closedSetNew, jUL.item)
                                closedsetNewTIDs = self._construct(closedsetNewTIDs, jUL)
                                if not self._isPassingHUIPruning(closedsetNewTIDs):
                                    passedHUIPruning = False
                                    break
                            else:
                                postsetNew.append(jUL)
                        else:
                            if self._containsAllTIDS(jUL, newgen_TIDs):
                                closedSetNew = self._appendItem(closedSetNew, jUL.item)
                                closedsetNewTIDs = self._construct(closedsetNewTIDs, jUL)
                                if not self._isPassingHUIPruning(closedsetNewTIDs):
                                    passedHUIPruning = False
                                    break
                            else:
                                postsetNew.append(jUL)

                        self.candidateCount += 1

                    if passedHUIPruning:
                        if closedsetNewTIDs.sumIutils >= self.minUtility:
                            self._saveCHUI(closedSetNew, closedsetNewTIDs.sumIutils, len(closedsetNewTIDs.elements))

                        presetNew = list(preset)
                        self._CLS_Miner(False, closedSetNew, closedsetNewTIDs, presetNew, postsetNew)

                    preset.append(iUL)

    def _isPassingHUIPruning(self, ul: UtilityList) -> bool:
        return (ul.sumIutils + ul.sumRutils) >= self.minUtility

    def _containsAllTIDS(self, ul1: UtilityList, ul2: UtilityList) -> bool:
        for ex in ul2.elements:
            ey = self._findElementWithTID(ul1, ex.tid)
            if ey is None:
                return False
        return True

    def _checkGenEUCPStrategy(self, itemX: int, itemY: int) -> bool:
        if self._compareItems(itemX, itemY) > 0:
            itemX, itemY = itemY, itemX
        mp = self.mapFMAP.get(itemX)
        if mp is not None:
            twuF = mp.get(itemY)
            if twuF is None or twuF < self.minUtility:
                return True
        return False

    def _appendItem(self, itemset: List[int], item: int) -> List[int]:
        return itemset + [int(item)]

    def _calculate_Con(self, X: UtilityList, Y: UtilityList) -> int:
        return int(X.sumIutils + X.sumRutils - self._cdiff(X, Y) * self._getMinValueofUL(X))

    def _cdiff(self, X: UtilityList, Y: UtilityList) -> int:
        if X.getSupport() < Y.getSupport():
            return 0
        return X.getSupport() - Y.getSupport()

    def _getMinValueofUL(self, X: UtilityList) -> int:
        if not X.elements:
            return 0
        minv = X.elements[0].iutils + X.elements[0].rutils
        for e in X.elements:
            v = e.iutils + e.rutils
            if v < minv:
                minv = v
        return minv

    def _improved_is_dup(self, newgenTIDs: UtilityList, preset: List[UtilityList]) -> bool:
        for j in preset:
            if self._preCheckContain(j, newgenTIDs):
                containsAll = True
                for ex in newgenTIDs.elements:
                    if self._findElementWithTID(j, ex.tid) is None:
                        containsAll = False
                        break
                if containsAll:
                    return True
        return False

    def _preCheckContain(self, X: UtilityList, Y: UtilityList) -> bool:
        lenX = X.getSupport()
        lenY = Y.getSupport()
        if lenX < lenY:
            return False
        for i in range(lenY):
            if X.elements[i].tid > Y.elements[i].tid:
                return False
            if X.elements[lenX - i - 1].tid < Y.elements[lenY - i - 1].tid:
                return False
        return True

    def _construct(self, uX: UtilityList, uE: UtilityList) -> UtilityList:
        uXE = UtilityList(uE.item)
        for elmX in uX.elements:
            elmE = self._findElementWithTID(uE, elmX.tid)
            if elmE is None:
                continue
            elmXe = Element(elmX.tid, elmX.iutils + elmE.iutils, elmX.rutils - elmE.iutils)
            uXE.addElement(elmXe)
        return uXE

    def _findElementWithTID(self, ulist: UtilityList, tid: int) -> Optional[Element]:
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

    def _CoverageConstructProcedure(self, listOfUtilityLists: List[UtilityList]) -> None:
        for ulX in listOfUtilityLists:
            itemX = ulX.item
            mapTWUF = self.mapFMAP.get(itemX)
            listofX: List[int] = []
            for ulY in listOfUtilityLists:
                itemY = ulY.item
                if itemX == itemY or self._compareItems(itemY, itemX) < 0:
                    continue
                if mapTWUF is not None and itemY in mapTWUF:
                    eucs_xy = mapTWUF[itemY]
                    if eucs_xy == self.mapItemToTWU.get(itemX, 0):
                        listofX.append(itemY)
            self.Cov[itemX] = listofX

    def _ifBelongToCov(self, x: int, y: int) -> bool:
        return y in self.Cov.get(x, [])

    def _saveCHUI(self, itemset: List[int], sumIutils: int, support: int) -> None:
        self.chuidCount += 1

        if self._writer is None:
            if self.listItemsetsBySize is None or self.setOfItemsInClosedItemsets is None:
                self.listItemsetsBySize = []
                self.setOfItemsInClosedItemsets = set()
            self._saveToMemory(itemset, sumIutils, support)
            return

        # Keep Java's trailing space after last item
        buf = []
        for it in itemset:
            buf.append(f"{it} ")
        buf.append(f"#SUP: {support} #UTIL: {sumIutils}")
        self._writer.write("".join(buf))
        self._writer.write("\n")

    def _saveToMemory(self, itemset: List[int], sumIutils: int, support: int) -> None:
        assert self.listItemsetsBySize is not None
        assert self.setOfItemsInClosedItemsets is not None

        k = len(itemset)
        while len(self.listItemsetsBySize) <= k:
            self.listItemsetsBySize.append([])

        self.listItemsetsBySize[k].append(Itemset(tuple(itemset), int(sumIutils), int(support)))
        for it in itemset:
            self.setOfItemsInClosedItemsets.add(it)

    def _compareItems(self, item1: int, item2: int) -> int:
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else compare

    def printStats(self) -> None:
        print("=============  CLS-Miner ALGORITHM SPMF 1.0 - STATS =============")
        print(f" Total time ~ {self.endTimestamp - self.startTimestamp} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" Closed High-utility itemsets count : {self.chuidCount}")
        print(f" Candidate count : {self.candidateCount}")
        print("=====================================================")


# ----------------------------------------------------------------------
# Helper: locate file like Java getResource()
# ----------------------------------------------------------------------
def file_to_path(filename: str) -> str:
    here = Path(__file__).resolve().parent
    candidates = [
        here / filename,
        here / "Java" / "src" / filename,
        Path.cwd() / filename,
        Path.cwd() / "Java" / "src" / filename,
    ]
    for p in candidates:
        if p.exists():
            return str(p.resolve())
    tried = "\n".join([f"- {c.resolve()}" for c in candidates])
    raise FileNotFoundError(f"Could not locate {filename}. Tried:\n{tried}")


# ----------------------------------------------------------------------
# Main (equivalent to MainTestCLS_Miner.java)
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="CLS-Miner (single-file Python port)")
    parser.add_argument("-i", "--input", help="Input utility DB file (e.g., DB_Utility.txt)")
    parser.add_argument("-s", "--support", type=int, help="Min utility threshold (int)")
    parser.add_argument("-o", "--output", help="Output file path (e.g., Java/src/output_py.txt)")

    args = parser.parse_args()

    # ✅ If no arguments are provided → behave like Java MainTest
    if not args.input and args.support is None and not args.output:
        print("No arguments provided. Running in default MainTest mode...\n")
        input_path = file_to_path("DB_Utility.txt")
        minutil = 30
        output_path = str((Path("Java") / "src" / "output_py.txt").resolve())
    else:
        if not args.input or args.support is None or not args.output:
            parser.error("Provide all of -i/--input, -s/--support, -o/--output OR run without arguments.")
        input_path = args.input
        minutil = int(args.support)
        output_path = args.output

    algo = AlgoCLS_miner(True, False, True, True)
    algo.runAlgorithm(input_path, minutil, output_path)
    algo.printStats()


if __name__ == "__main__":
    main()
