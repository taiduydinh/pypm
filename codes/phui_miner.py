#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Single-file Python conversion of SPMF AlgoPHUIMiner (PHUI-Miner).

Matches MainTestPHUIMiner.java defaults:
- lminutil = 40
- lamda (lambda) = 2
- windowSize = 3
- input: DB_LHUI.txt
- output: output_py.txt (change if you want "Java/src/output_java.txt")

Expected input line format (4 fields separated by ':'):
items : transactionUtility : itemUtilities : timestamp
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import os
import time


# -----------------------------
# Memory Logger (best-effort)
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
        self.iutilPeriod: List[Period] = []  # iutil >= threshold
        self.utilPeriod: List[Period] = []   # iutil+rutil >= threshold


class UtilityListPeak(UtilityListLHUI):
    def __init__(self, item: int) -> None:
        super().__init__(item)
        self.peak: List[Period] = []         # peak windows


@dataclass
class TWUPair:
    currentTWU: int = 0
    maxTWU: int = 0


# -----------------------------
# AlgoPHUIMiner (converted)
# -----------------------------
class AlgoPHUIMiner:
    BUFFERS_SIZE = 200

    class Pair:
        __slots__ = ("item", "utility")

        def __init__(self, item: int, utility: int) -> None:
            self.item = item
            self.utility = utility

    def __init__(self) -> None:
        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0
        self.huiCount: int = 0
        self.joinCount: int = 0

        self.mapItemToTWU: Dict[int, TWUPair] = {}
        self.timeTid: List[int] = []  # timestamp per transaction by tid order

        self.itemsetBuffer: List[int] = [0] * self.BUFFERS_SIZE
        self._writer = None  # type: ignore

    # ---------- helpers ----------
    @staticmethod
    def _read_transactions(input_path: str) -> List[Tuple[List[int], int, List[int], int]]:
        """
        Returns list of (items, transactionUtility, itemUtilities, timestamp) in file order,
        skipping comments/metadata lines.
        """
        txs: List[Tuple[List[int], int, List[int], int]] = []
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                split = line.split(":")
                items = [int(x) for x in split[0].split()]
                tu = int(split[1])
                utils = [int(x) for x in split[2].split()]
                ts = int(split[3])
                txs.append((items, tu, utils, ts))
        return txs

    def _compute_max_local_twu(self, txs: List[Tuple[List[int], int, List[int], int]], window: int) -> None:
        """
        Compute per-item max TWU over any time-based window of length 'window',
        matching the intent of the Java two-pointer implementation.
        """
        self.mapItemToTWU.clear()
        self.timeTid = [ts for (_, _, _, ts) in txs]

        current: Dict[int, int] = {}
        maxv: Dict[int, int] = {}

        left = 0
        for right in range(len(txs)):
            items_r, tu_r, _, ts_r = txs[right]

            # Add current transaction to window
            for it in items_r:
                current[it] = current.get(it, 0) + tu_r
                if current[it] > maxv.get(it, 0):
                    maxv[it] = current[it]

            # Shrink window while outside: ts_r >= ts_left + window  (strictly matches break condition in Java)
            while left <= right and ts_r >= txs[left][3] + window:
                items_l, tu_l, _, _ = txs[left]
                for it in items_l:
                    current[it] = current.get(it, 0) - tu_l
                left += 1

        # Store results
        for it, mx in maxv.items():
            self.mapItemToTWU[it] = TWUPair(currentTWU=0, maxTWU=mx)

    def compareItems(self, item1: int, item2: int) -> int:
        compare = self.mapItemToTWU[item1].maxTWU - self.mapItemToTWU[item2].maxTWU
        return (item1 - item2) if compare == 0 else int(compare)

    # ---------- algorithm ----------
    def runAlgorithm(self, input_path: str, output_path: str, minUtility: int, window: int, lamda: float) -> None:
        MemoryLogger.getInstance().reset()
        self.huiCount = 0
        self.joinCount = 0

        self.startTimestamp = time.time() * 1000.0
        self.itemsetBuffer = [0] * self.BUFFERS_SIZE

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        self._writer = open(output_path, "w", encoding="utf-8")

        # Read all transactions once
        txs = self._read_transactions(input_path)

        # First pass: compute max local TWU per item (time-based windows)
        self._compute_max_local_twu(txs, window)

        # Build list of promising items (maxTWU >= minUtility)
        listOfUtilityListPeaks: List[UtilityListPeak] = []
        mapItemToUtilityListPeak: Dict[int, UtilityListPeak] = {}

        for item, twup in self.mapItemToTWU.items():
            if twup.maxTWU >= minUtility:
                ul = UtilityListPeak(item)
                mapItemToUtilityListPeak[item] = ul
                listOfUtilityListPeaks.append(ul)

        listOfUtilityListPeaks.sort(key=lambda ul: (self.mapItemToTWU[ul.item].maxTWU, ul.item))

        # Second pass: build utility lists for promising items
        tid = 0
        for items, _, utils, _ in txs:
            remainingUtility = 0
            revised: List[AlgoPHUIMiner.Pair] = []

            for i in range(len(items)):
                it = items[i]
                u = utils[i]
                if self.mapItemToTWU.get(it, TWUPair()).maxTWU >= minUtility:
                    revised.append(AlgoPHUIMiner.Pair(it, u))
                    remainingUtility += u

            revised.sort(key=lambda p: (self.mapItemToTWU[p.item].maxTWU, p.item))

            for pair in revised:
                remainingUtility -= pair.utility
                ul_item = mapItemToUtilityListPeak[pair.item]
                ul_item.addElement(Element(tid, pair.utility, remainingUtility))

            tid += 1

        # Generate peaks for 1-item utility lists
        for ulp in listOfUtilityListPeaks:
            self.generatePeak(ulp, minUtility, window, lamda)

        MemoryLogger.getInstance().checkMemory()

        # Mine recursively
        self.phuiMiner(self.itemsetBuffer, 0, None, listOfUtilityListPeaks, minUtility, window, lamda)

        MemoryLogger.getInstance().checkMemory()

        self._writer.close()
        self._writer = None
        self.endTimestamp = time.time() * 1000.0

    def phuiMiner(
        self,
        prefix: List[int],
        prefixLength: int,
        pUL: Optional[UtilityListPeak],
        ULs: List[UtilityListPeak],
        minUtility: int,
        window: int,
        lamda: float,
    ) -> None:
        for i in range(len(ULs)):
            X = ULs[i]

            # output if has peak windows
            if X.peak:
                self.writeOut(prefix, prefixLength, X)

            # recurse only if promising periods exist
            if X.utilPeriod:
                exULs: List[UtilityListPeak] = []
                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    pXY = self.construct(pUL, X, Y)
                    self.generatePeak(pXY, minUtility, window, lamda)
                    exULs.append(pXY)
                    self.joinCount += 1

                prefix[prefixLength] = X.item
                self.phuiMiner(prefix, prefixLength + 1, X, exULs, minUtility, window, lamda)

    def construct(self, P: Optional[UtilityListPeak], px: UtilityListPeak, py: UtilityListPeak) -> UtilityListPeak:
        pxyUL = UtilityListPeak(py.item)
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
    def findElementWithTID(ulist: UtilityListPeak, tid: int) -> Optional[Element]:
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

    def writeOut(self, prefix: List[int], prefixLength: int, ulp: UtilityListPeak) -> None:
        self.huiCount += 1

        parts: List[str] = []
        for i in range(prefixLength):
            parts.append(str(prefix[i]))
        parts.append(str(ulp.item))

        # Java: "<items> #UTIL: <sumIutils> peak windows: [a,b] [c,d] ..."
        line = " ".join(parts) + " #UTIL: " + str(ulp.sumIutils) + " peak windows: "

        for p in ulp.peak:
            start_tid = ulp.elements[p.beginIndex].tid
            end_tid = ulp.elements[p.endIndex].tid
            line += f"[{self.timeTid[start_tid]},{self.timeTid[end_tid]}] "

        assert self._writer is not None
        self._writer.write(line.rstrip() + "\n")

    # ---------- Peak generation ----------
    def generatePeak(self, ulp: UtilityListPeak, minutil: int, window: int, lamda: float) -> None:
        ulp.iutilPeriod.clear()
        ulp.utilPeriod.clear()
        ulp.peak.clear()

        if not ulp.elements:
            return

        iutil = 0
        rutil = 0
        putil = 0
        winEnd = 0
        win2start = 0

        iutilPreflag = False
        utilPreflag = False
        putilPreflag = False

        win2len = int(window / lamda)
        if win2len <= 0:
            win2len = 1

        t0 = self.timeTid[ulp.elements[0].tid]

        # scan elements in first big window but not in first small window
        limit1 = t0 + (window - win2len)
        while winEnd < len(ulp.elements) and self.timeTid[ulp.elements[winEnd].tid] < limit1:
            iutil += ulp.elements[winEnd].iutils
            rutil += ulp.elements[winEnd].rutils
            winEnd += 1

        win2start = winEnd

        # scan elements in both windows
        limit2 = t0 + window
        while winEnd < len(ulp.elements) and self.timeTid[ulp.elements[winEnd].tid] < limit2:
            iutil += ulp.elements[winEnd].iutils
            putil += ulp.elements[winEnd].iutils
            rutil += ulp.elements[winEnd].rutils
            winEnd += 1

        if iutil >= minutil:
            iutilPreflag = True
        if iutil + rutil >= minutil:
            utilPreflag = True
        if (float(putil) * window / win2len) > iutil and iutil >= minutil:
            putilPreflag = True

        self.slideWindow(ulp, winEnd, minutil, iutil, iutilPreflag, rutil, utilPreflag,
                         putil, win2start, putilPreflag, window, win2len)

    def slideWindow(
        self,
        ulp: UtilityListPeak,
        winEnd: int,
        minutil: int,
        iutil: int,
        iutilPreflag: bool,
        rutil: int,
        utilPreflag: bool,
        putil: int,
        win2start: int,
        putilPreflag: bool,
        window: int,
        win2len: int,
    ) -> None:
        beginIndex, endIndex = 0, winEnd
        uBeginIndex, uEndIndex = 0, winEnd

        pBeginIndex = winEnd - 1
        pEndIndex = winEnd - 1

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

            # extend big window
            if y < len(ulp.elements):
                ty = self.timeTid[ulp.elements[y].tid]
                while winEnd < len(ulp.elements) and self.timeTid[ulp.elements[winEnd].tid] < ty + window:
                    iutil += ulp.elements[winEnd].iutils
                    putil += ulp.elements[winEnd].iutils
                    rutil += ulp.elements[winEnd].rutils
                    winEnd += 1

            # shift small window start
            if winEnd > 0:
                last_time = self.timeTid[ulp.elements[winEnd - 1].tid]
                while win2start < len(ulp.elements) and self.timeTid[ulp.elements[win2start].tid] < (last_time - win2len):
                    putil -= ulp.elements[win2start].iutils
                    win2start += 1

            # iutil periods
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

            # promising periods (iutil + rutil)
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

            # peak windows: putil*(window/win2len) >= iutil and iutil >= minutil
            if putilPreflag:
                if (float(putil) * window / win2len) < iutil or iutil < minutil:
                    ulp.peak.append(Period(pBeginIndex, pEndIndex))
                    putilPreflag = False
                else:
                    pEndIndex = winEnd - 1
            else:
                if (float(putil) * window / win2len) >= iutil and iutil >= minutil:
                    putilPreflag = True
                    pBeginIndex = winEnd - 1
                    pEndIndex = winEnd - 1

    def printStats(self) -> None:
        print("=============  PHUI-MINER ALGORITHM - STATS =============")
        print(f" Total time ~ {int(self.endTimestamp - self.startTimestamp)} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" Peak High-utility itemsets count : {self.huiCount}")
        print(f" Join count : {self.joinCount}")
        print("===================================================")


# -----------------------------
# Main (like MainTestPHUIMiner)
# -----------------------------
def main() -> None:
    lminutil = 35
    lamda = 2
    windowSize = 3

    input_file = "Java/src/DB_LHUI.txt"
    output_file = "Java/src/output_py.txt"  # change to "Java/src/output_java.txt" if you want same path

    algo = AlgoPHUIMiner()
    algo.runAlgorithm(input_file, output_file, lminutil, windowSize, lamda)
    algo.printStats()


if __name__ == "__main__":
    main()