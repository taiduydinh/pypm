#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Single-file Python conversion of SPMF AlgoLHUIMiner (LHUI-Miner).

Matches MainTestLHUIMiner.java defaults:
- lminutil = 40
- windowSize = 3
- input: DB_LHUI.txt
- output: output_py.txt (change if you want the same path as Java)

Input line format expected (4 fields separated by ':'):
items : transactionUtility : itemUtilities : timestamp

Output format (like Java):
<itemset> #UTIL: <sumIutils> [startTime,endTime] [startTime,endTime] ...
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional
import os
import time


# -----------------------------
# Memory Logger (lightweight)
# -----------------------------
class MemoryLogger:
    _instance = None

    def __init__(self) -> None:
        self._max_mb: float = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self._max_mb = 0.0

    def checkMemory(self) -> None:
        # Best-effort cross-platform memory usage
        try:
            import resource  # type: ignore
            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Linux: KB, macOS: bytes (often)
            mb = usage / 1024.0 if usage < 10_000_000 else usage / 1024.0 / 1024.0
            if mb > self._max_mb:
                self._max_mb = mb
        except Exception:
            pass

    def getMaxMemory(self) -> float:
        return self._max_mb


# -----------------------------
# Core structures
# -----------------------------
@dataclass
class Element:
    tid: int
    iutils: int
    rutils: int


@dataclass
class Period:
    beginIndex: int
    endIndex: int


class UtilityList:
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


class UtilityListLHUI(UtilityList):
    def __init__(self, item: int) -> None:
        super().__init__(item)
        self.iutilPeriod: List[Period] = []  # periods where iutil >= threshold
        self.utilPeriod: List[Period] = []   # periods where iutil+rutil >= threshold


# -----------------------------
# AlgoLHUIMiner (converted)
# -----------------------------
class AlgoLHUIMiner:
    class Pair:
        __slots__ = ("item", "utility")

        def __init__(self, item: int, utility: int) -> None:
            self.item = item
            self.utility = utility

    BUFFERS_SIZE = 200

    def __init__(self) -> None:
        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0

        self.huiCount: int = 0
        self.candidateCount: int = 0
        self.joinCount: int = 0

        self.mapItemToTWU: Dict[int, int] = {}
        self.timeTid: List[int] = []  # timestamp per transaction (by tid order)

        self.itemsetBuffer: List[int] = [0] * self.BUFFERS_SIZE
        self._writer = None  # type: ignore

    def runAlgorithm(self, input_path: str, output_path: str, minUtility: int, window: int) -> None:
        MemoryLogger.getInstance().reset()
        self.huiCount = 0
        self.candidateCount = 0
        self.joinCount = 0
        self.timeTid = []
        self.mapItemToTWU = {}

        self.startTimestamp = time.time() * 1000.0

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        self._writer = open(output_path, "w", encoding="utf-8")

        # -------- First pass: TWU + collect timestamps --------
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = split[0].split()
                transactionUtility = int(split[1])
                timestamp = int(split[3])
                self.timeTid.append(timestamp)

                for it in items:
                    item = int(it)
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + transactionUtility

        # -------- Create utility lists for promising 1-items --------
        listOfUtilityListPeaks: List[UtilityListLHUI] = []
        mapItemToUtilityListPeak: Dict[int, UtilityListLHUI] = {}

        for item, twu in self.mapItemToTWU.items():
            if twu >= minUtility:
                ul = UtilityListLHUI(item)
                mapItemToUtilityListPeak[item] = ul
                listOfUtilityListPeaks.append(ul)

        listOfUtilityListPeaks.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        # -------- Second pass: build utility lists --------
        tid = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = split[0].split()
                utilityValues = split[2].split()

                remainingUtility = 0
                revised: List[AlgoLHUIMiner.Pair] = []

                for i in range(len(items)):
                    item = int(items[i])
                    util = int(utilityValues[i])
                    if self.mapItemToTWU.get(item, 0) >= minUtility:
                        revised.append(AlgoLHUIMiner.Pair(item, util))
                        remainingUtility += util

                revised.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                for pair in revised:
                    remainingUtility -= pair.utility
                    ul_item = mapItemToUtilityListPeak[pair.item]
                    ul_item.addElement(Element(tid, pair.utility, remainingUtility))

                tid += 1

        # -------- Generate periods for 1-item utility lists --------
        for ul in listOfUtilityListPeaks:
            self.generatePeriod(ul, minUtility, window)

        MemoryLogger.getInstance().checkMemory()

        # -------- Mine recursively --------
        self.lhuiMiner(self.itemsetBuffer, 0, None, listOfUtilityListPeaks, minUtility, window)

        MemoryLogger.getInstance().checkMemory()

        self._writer.close()
        self._writer = None
        self.endTimestamp = time.time() * 1000.0

    def compareItems(self, item1: int, item2: int) -> int:
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else int(compare)

    def lhuiMiner(
        self,
        prefix: List[int],
        prefixLength: int,
        pUL: Optional[UtilityListLHUI],
        ULs: List[UtilityListLHUI],
        minUtility: int,
        window: int,
    ) -> None:
        for i in range(len(ULs)):
            X = ULs[i]

            # If LHUI periods of pX not empty => output pX
            if X.iutilPeriod:
                self.writeOut(prefix, prefixLength, X)

            # If promising periods not empty => explore extensions
            if X.utilPeriod:
                exULs: List[UtilityListLHUI] = []
                self.candidateCount += 1  # matches Java (increment once per X that expands)

                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    pXY = self.construct(pUL, X, Y)
                    self.generatePeriod(pXY, minUtility, window)
                    exULs.append(pXY)
                    self.joinCount += 1

                prefix[prefixLength] = X.item
                self.lhuiMiner(prefix, prefixLength + 1, X, exULs, minUtility, window)

    def construct(self, P: Optional[UtilityListLHUI], px: UtilityListLHUI, py: UtilityListLHUI) -> UtilityListLHUI:
        pxyUL = UtilityListLHUI(py.item)

        for ex in px.elements:
            ey = self.findElementWithTID(py, ex.tid)
            if ey is None:
                continue

            if P is None:
                pxyUL.addElement(Element(ex.tid, ex.iutils + ey.iutils, ey.rutils))
            else:
                e = self.findElementWithTID(P, ex.tid)
                if e is not None:
                    pxyUL.addElement(Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils))

        return pxyUL

    @staticmethod
    def findElementWithTID(ulist: UtilityListLHUI, tid: int) -> Optional[Element]:
        arr = ulist.elements
        first, last = 0, len(arr) - 1
        while first <= last:
            mid = (first + last) >> 1
            mtid = arr[mid].tid
            if mtid < tid:
                first = mid + 1
            elif mtid > tid:
                last = mid - 1
            else:
                return arr[mid]
        return None

    def writeOut(self, prefix: List[int], prefixLength: int, ulp: UtilityListLHUI) -> None:
        self.huiCount += 1

        parts = []
        for i in range(prefixLength):
            parts.append(str(prefix[i]))
        parts.append(str(ulp.item))

        line = " ".join(parts) + " #UTIL: " + str(ulp.sumIutils) + " "

        # Append periods as [startTime,endTime]
        for p in ulp.iutilPeriod:
            start_tid = ulp.elements[p.beginIndex].tid
            end_tid = ulp.elements[p.endIndex].tid
            line += f"[{self.timeTid[start_tid]},{self.timeTid[end_tid]}] "

        assert self._writer is not None
        self._writer.write(line.rstrip() + "\n")

    def generatePeriod(self, ulp: UtilityListLHUI, minutil: int, window: int) -> None:
        # Clear old periods (important if ulp reused)
        ulp.iutilPeriod.clear()
        ulp.utilPeriod.clear()

        if not ulp.elements:
            return

        iutil = 0
        rutil = 0
        winEnd = 0

        # first window: timestamps < t0 + window
        t0 = self.timeTid[ulp.elements[0].tid]
        while winEnd < len(ulp.elements) and self.timeTid[ulp.elements[winEnd].tid] < t0 + window:
            iutil += ulp.elements[winEnd].iutils
            rutil += ulp.elements[winEnd].rutils
            winEnd += 1

        iutilPreflag = (iutil >= minutil)
        utilPreflag = (iutil + rutil >= minutil)

        self.slideWindow(ulp, winEnd, minutil, iutil, iutilPreflag, rutil, utilPreflag, window)

    def slideWindow(
        self,
        ulp: UtilityListLHUI,
        winEnd: int,
        minutil: int,
        iutil: int,
        iutilPreflag: bool,
        rutil: int,
        utilPreflag: bool,
        window: int,
    ) -> None:
        beginIndex, endIndex = 0, winEnd
        uBeginIndex, uEndIndex = 0, winEnd

        i = 0
        while i < len(ulp.elements):
            # remove all elements with the same timestamp as elements[i]
            y = i
            ti = self.timeTid[ulp.elements[i].tid]
            while y < len(ulp.elements) and self.timeTid[ulp.elements[y].tid] == ti:
                iutil -= ulp.elements[y].iutils
                rutil -= ulp.elements[y].rutils
                y += 1
            i = y

            # expand window to include timestamps < time(elements[y]) + window
            x = winEnd
            if y < len(ulp.elements):
                ty = self.timeTid[ulp.elements[y].tid]
                while x < len(ulp.elements) and self.timeTid[ulp.elements[x].tid] < ty + window:
                    iutil += ulp.elements[x].iutils
                    rutil += ulp.elements[x].rutils
                    x += 1
                winEnd = x

            # LHUI periods: iutil >= minutil
            if iutilPreflag:
                if iutil < minutil:
                    ulp.iutilPeriod.append(Period(beginIndex, endIndex - 1))
                    iutilPreflag = False
                else:
                    endIndex = winEnd
            else:
                if iutil >= minutil:
                    iutilPreflag = True
                    beginIndex = i
                    endIndex = winEnd

            # Promising periods: iutil + rutil >= minutil
            if utilPreflag:
                if iutil + rutil < minutil:
                    ulp.utilPeriod.append(Period(uBeginIndex, uEndIndex - 1))
                    utilPreflag = False
                else:
                    uEndIndex = winEnd
            else:
                if iutil + rutil >= minutil:
                    utilPreflag = True
                    uBeginIndex = i
                    uEndIndex = winEnd

    def printStats(self) -> None:
        print("=============  LHUI-MINER ALGORITHM - STATS =============")
        print(f" Total time ~ {int(self.endTimestamp - self.startTimestamp)} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" Local High-utility itemsets count : {self.huiCount}")
        print(f" Join count : {self.joinCount}")
        print(f" Candidate count : {self.candidateCount}")
        print("===================================================")


# -----------------------------
# Main (like MainTestLHUIMiner)
# -----------------------------
def main() -> None:
    lminutil = 45
    windowSize = 3

    input_file = "Java//src//DB_LHUI.txt"
    output_file = "Java//src//output_py.txt"  # change to "Java/src/output_java.txt" if you want same path

    algo = AlgoLHUIMiner()
    algo.runAlgorithm(input_file, output_file, lminutil, windowSize)
    algo.printStats()


if __name__ == "__main__":
    main()