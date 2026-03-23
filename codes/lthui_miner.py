# lthui_miner.py
# Python port of SPMF AlgoLTHUIMiner (LTHUI-Miner)
# Based on Java code by Yanjun Yang, Philippe Fournier-Viger (PAKDD 2020)
# Ported to Python with the same control flow and output format.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import time
import math
import bisect
import os
import resource


# -----------------------------
# Utility / Memory logger
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
        # ru_maxrss is KB on Linux, bytes on macOS. We'll handle both.
        ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Heuristic: if extremely large, it's probably bytes; else KB.
        # On macOS ru_maxrss is bytes; on Linux it's KB.
        mb = (ru / (1024 * 1024)) if ru > 10_000_000 else (ru / 1024.0)
        if mb > self._max_mb:
            self._max_mb = mb

    def getMaxMemory(self) -> float:
        return self._max_mb


# -----------------------------
# Core data structures
# -----------------------------
@dataclass
class Element:
    tid: int
    utils: int
    rutils: int


@dataclass
class Period:
    beginIndex: int
    endIndex: int


class TrendUtilityList:
    """
    Inferred from usage in AlgoLTHUIMiner.java
    """
    def __init__(self, item: int, num_bin: int, num_win: int) -> None:
        self.item: int = item
        self.elements: List[Element] = []

        # Per-bin utility / remaining utility
        self.utilBin: List[int] = [0] * num_bin
        self.rutilBin: List[int] = [0] * num_bin

        # Trending periods + avg slope per period
        self.trendPeriod: List[Period] = []
        self.trendSlope: List[float] = []

        # Promising periods (based on sumUtil + sumRutil)
        self.rutilPeriod: List[Period] = []

        # Sliding-window "remain" bitset used by property3 pruning
        # In Java they do: if (p != null && p.winRemain.get(i) == false) continue;
        # and later: pX.winRemain.set(i);  -> meaning allow / keep.
        # Most consistent default is: all windows are allowed initially.
        self.winRemain: List[bool] = [True] * max(0, num_win)

        # Sums over all elements (used in property2 pruning)
        self.sumUtils: int = 0
        self.sumRutils: int = 0

    def addElement(self, e: Element) -> None:
        self.elements.append(e)
        self.sumUtils += e.utils
        self.sumRutils += e.rutils


# -----------------------------
# Algorithm implementation
# -----------------------------
class AlgoLTHUIMiner:
    BUFFERS_SIZE = 300

    @dataclass
    class Pair:
        item: int
        utility: int

    def __init__(self) -> None:
        self.startTimestamp = 0.0
        self.endTimestamp = 0.0

        self.lthuiCount = 0
        self.candidateCount = 0
        self.joinCount = 0

        self.mapItemToTWU: Dict[int, int] = {}
        self.timeDiff: List[float] = []

        self.minTime: int = 0
        self.numBin: int = 0
        self.numWin: int = 0
        self.numBinOfWin: int = 0
        self.dbLen: int = 0

        self.timeTid: List[int] = []
        self.binIndexTid: List[int] = []

        self.itemsetBuffer: List[int] = [0] * self.BUFFERS_SIZE

        self._out = None  # file handle

    # ---- Public API ----
    def run_algorithm(
        self,
        input_file: str,
        output_file: str,
        lminutil: int,
        winlen: int,
        binlen: int,
        minslope: float,
        database_start_timestamp: int,
        output_index: bool
    ) -> None:
        self.lthuiCount = 0
        self.candidateCount = 0
        self.joinCount = 0
        self.mapItemToTWU = {}
        self.timeTid = []
        self.binIndexTid = []

        mem = MemoryLogger.getInstance()
        mem.reset()

        self.startTimestamp = time.time()

        # user-set start timestamp
        if database_start_timestamp >= 0:
            self.minTime = int(database_start_timestamp)

        # number of bins in a window
        self.numBinOfWin = winlen // binlen

        # prepare timeDiff ( (t-avg) list + denominator at the end )
        self.timeDiff = []
        tmpSum = sum(range(1, self.numBinOfWin + 1))
        avg = tmpSum / float(self.numBinOfWin) if self.numBinOfWin > 0 else 0.0

        denom = 0.0
        for i in range(1, self.numBinOfWin + 1):
            d = i - avg
            self.timeDiff.append(d)
            denom += d * d
        self.timeDiff.append(denom)

        # open output
        self._out = open(output_file, "w", encoding="utf-8")

        # -------------------------
        # PRE DATABASE PASS
        # store maxTime, numBin, timeTid and dbLen
        # -------------------------
        maxTime = None
        self.dbLen = 0

        with open(input_file, "r", encoding="utf-8") as f:
            last_time = 0
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                self.dbLen += 1
                split = line.split(":")
                # timestamp in split[3]
                t = int(split[3])
                if self.dbLen == 1 and database_start_timestamp < 0:
                    self.minTime = t
                self.timeTid.append(t)
                last_time = t
            maxTime = last_time

        if maxTime is None:
            self._out.close()
            self.endTimestamp = time.time()
            return

        self.numBin = int((maxTime - self.minTime + 1) // binlen)
        # ignore trailing incomplete bin
        maxTime_effective = self.minTime + self.numBin * binlen - 1

        # -------------------------
        # FIRST DATABASE PASS
        # calculate binIndexTid and TWU
        # -------------------------
        self.dbLen = 0
        indexBin = 0
        nextBinStart = 0
        self.binIndexTid = []

        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = split[0].split()
                transactionUtility = int(split[1])
                t = int(split[3])

                if t > maxTime_effective:
                    break

                # TWU update
                for it in items:
                    item = int(it)
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + transactionUtility

                # compute nextBinStart at first tx
                if self.dbLen == 0:
                    nextBinStart = self.minTime + binlen * 1

                while t >= nextBinStart:
                    indexBin += 1
                    nextBinStart += binlen

                # store bin index for this tid
                self.binIndexTid.append(indexBin)
                self.dbLen += 1

        # sliding windows count
        self.numWin = self.numBin - self.numBinOfWin + 1
        if self.numWin < 1:
            self._out.close()
            self.endTimestamp = time.time()
            return

        # -------------------------
        # Create TU-lists for items with TWU >= lminutil
        # -------------------------
        listOfTUList: List[TrendUtilityList] = []
        mapItemToTUList: Dict[int, TrendUtilityList] = {}

        for item, twu in self.mapItemToTWU.items():
            if twu >= lminutil:
                tul = TrendUtilityList(item, self.numBin, self.numWin)
                mapItemToTUList[item] = tul
                listOfTUList.append(tul)

        # sort by compareItems (TWU asc then item asc)
        listOfTUList.sort(key=lambda tul: (self.mapItemToTWU[tul.item], tul.item))

        # -------------------------
        # SECOND DATABASE PASS
        # Construct TU-lists for promising 1-itemsets
        # -------------------------
        tid = 0
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = split[0].split()
                utils = split[2].split()
                t = int(split[3])

                if t > maxTime_effective:
                    break

                remainingUtility = 0
                revised: List[AlgoLTHUIMiner.Pair] = []

                for i in range(len(items)):
                    item = int(items[i])
                    u = int(utils[i])
                    # pruning property1: low-TWU item removed
                    if self.mapItemToTWU.get(item, 0) >= lminutil:
                        revised.append(self.Pair(item=item, utility=u))
                        remainingUtility += u

                # sort revised tx by compareItems
                revised.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                # add elements
                for p in revised:
                    remainingUtility -= p.utility
                    tul = mapItemToTUList.get(p.item)
                    if tul is None:
                        continue
                    tul.addElement(Element(tid=tid, utils=p.utility, rutils=remainingUtility))

                tid += 1

        # -------------------------
        # Generate bin infos + periods for 1-itemsets
        # -------------------------
        for tul in listOfTUList:
            self._cal_bin_infos(tul)
            self._find_trend(p=None, pX=tul, lminutil=lminutil, winlen=winlen, binlen=binlen, minslope=minslope)

        mem.checkMemory()

        # -------------------------
        # Recursive mining
        # -------------------------
        self._lthui_search(
            prefix=[],
            pUL=None,
            ULs=listOfTUList,
            lminutil=lminutil,
            winlen=winlen,
            binlen=binlen,
            minslope=minslope,
            output_index=output_index
        )

        mem.checkMemory()
        self._out.close()
        self.endTimestamp = time.time()

    def print_stats(self) -> None:
        print("=============  LTHUI-MINER ALGORITHM v2.44 - STATS =============")
        print(f" Total time ~ {int((self.endTimestamp - self.startTimestamp) * 1000)} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory():.3f} MB")
        print(f" Locally Trending High-utility itemsets count : {self.lthuiCount}")
        print(f" Join count : {self.joinCount}")
        print(f" Candidate count : {self.candidateCount}")
        print("===================================================")

    # ---- Internal helpers ----
    def _lthui_search(
        self,
        prefix: List[int],
        pUL: Optional[TrendUtilityList],
        ULs: List[TrendUtilityList],
        lminutil: int,
        winlen: int,
        binlen: int,
        minslope: float,
        output_index: bool
    ) -> None:
        for i in range(len(ULs)):
            X = ULs[i]

            # If trend periods of pX is not empty => output
            if X.trendPeriod:
                self._write_out(prefix, X, binlen, output_index)

            # Property2 pruning
            if X.rutilPeriod and (X.sumUtils + X.sumRutils >= lminutil):
                exULs: List[TrendUtilityList] = []
                self.candidateCount += 1

                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    pXY = self._construct(pUL, X, Y)
                    self._cal_bin_infos(pXY)
                    self._find_trend(p=X, pX=pXY, lminutil=lminutil, winlen=winlen, binlen=binlen, minslope=minslope)
                    exULs.append(pXY)
                    self.joinCount += 1

                self._lthui_search(
                    prefix=prefix + [X.item],
                    pUL=X,
                    ULs=exULs,
                    lminutil=lminutil,
                    winlen=winlen,
                    binlen=binlen,
                    minslope=minslope,
                    output_index=output_index
                )

    def _construct(self, P: Optional[TrendUtilityList], px: TrendUtilityList, py: TrendUtilityList) -> TrendUtilityList:
        pxy = TrendUtilityList(py.item, self.numBin, self.numWin)

        # elements are in tid order (added sequentially), so we can binary search in py
        py_tids = [e.tid for e in py.elements]

        if P is None:
            for ex in px.elements:
                idx = bisect.bisect_left(py_tids, ex.tid)
                if idx >= len(py_tids) or py_tids[idx] != ex.tid:
                    continue
                ey = py.elements[idx]
                pxy.addElement(Element(tid=ex.tid, utils=ex.utils + ey.utils, rutils=ey.rutils))
        else:
            P_tids = [e.tid for e in P.elements]
            for ex in px.elements:
                idx_y = bisect.bisect_left(py_tids, ex.tid)
                if idx_y >= len(py_tids) or py_tids[idx_y] != ex.tid:
                    continue
                ey = py.elements[idx_y]

                idx_p = bisect.bisect_left(P_tids, ex.tid)
                if idx_p >= len(P_tids) or P_tids[idx_p] != ex.tid:
                    continue
                e = P.elements[idx_p]

                pxy.addElement(Element(tid=ex.tid, utils=ex.utils + ey.utils - e.utils, rutils=ey.rutils))

        return pxy

    def _find_trend(self, p: Optional[TrendUtilityList], pX: TrendUtilityList, lminutil: int, winlen: int, binlen: int, minslope: float) -> None:
        winEnd = self.numBinOfWin
        k = winlen // binlen

        zeroAble = k // 2  # default 0.5 proportion
        countZero = 0

        winStart = 0
        while winStart < self.numBin and (winEnd - 1) < self.numBin:
            trendPreFlag = False
            rutilPreFlag = False

            numSliding = 0
            sumSlope = 0.0

            sumUtil = 0
            sumRutil = 0
            countBin = 0
            countZero = 0

            i = winStart
            while i < self.numBin and countBin != self.numBinOfWin:
                if countZero <= zeroAble:
                    if countBin == 0:
                        winStart = i
                        # property3 pruning
                        if p is not None and (winStart < len(p.winRemain)) and (p.winRemain[winStart] is False):
                            i += 1
                            continue
                    if pX.utilBin[i] == 0:
                        countZero += 1
                    sumUtil += pX.utilBin[i]
                    sumRutil += pX.rutilBin[i]
                    countBin += 1
                    i += 1
                else:
                    countBin = 0
                    countZero = 0
                    sumUtil = 0
                    sumRutil = 0
                    i = winStart + 1
                    winStart = i

            if countBin != self.numBinOfWin:
                break

            if sumUtil + sumRutil >= lminutil:
                rutilPreFlag = True
            if sumUtil >= lminutil:
                slope = self._cal_slope(pX, winStart)
                if slope >= minslope:
                    numSliding += 1
                    sumSlope += slope
                    trendPreFlag = True

            winEnd = winStart + self.numBinOfWin
            beginIndex = winStart
            endIndex = winEnd
            rBeginIndex = winStart
            rEndIndex = winEnd

            while winStart < self.numBin and winEnd < self.numBin:
                # property3 pruning
                if p is not None and (winStart + 1) < len(p.winRemain) and (p.winRemain[winStart + 1] is False):
                    winStart += 1
                    winEnd += 1
                    break

                # update zero bins
                if pX.utilBin[winEnd] == 0:
                    countZero += 1
                if pX.utilBin[winStart] == 0:
                    countZero -= 1
                if countZero > zeroAble:
                    break

                # slide left
                sumUtil -= pX.utilBin[winStart]
                sumRutil -= pX.rutilBin[winStart]
                # slide right
                sumUtil += pX.utilBin[winEnd]
                sumRutil += pX.rutilBin[winEnd]

                winStart += 1
                winEnd += 1

                # trendPeriod logic
                if trendPreFlag:
                    slope = self._cal_slope(pX, winStart)
                    if sumUtil < lminutil or slope < minslope:
                        pX.trendPeriod.append(Period(beginIndex, endIndex - 1))
                        pX.trendSlope.append(sumSlope / numSliding if numSliding else 0.0)
                        numSliding = 0
                        sumSlope = 0.0
                        trendPreFlag = False
                    else:
                        numSliding += 1
                        sumSlope += slope
                        endIndex = winEnd
                else:
                    slope = self._cal_slope(pX, winStart)
                    if sumUtil >= lminutil and slope >= minslope:
                        beginIndex = winStart
                        endIndex = winEnd
                        numSliding += 1
                        sumSlope += slope
                        trendPreFlag = True

                # rutilPeriod logic
                if rutilPreFlag:
                    if (sumUtil + sumRutil) < lminutil:
                        pX.rutilPeriod.append(Period(rBeginIndex, rEndIndex - 1))
                        # set winRemain for valid starts inside this rutil period
                        last_start = (rEndIndex - 1) - self.numBinOfWin + 1
                        for s in range(rBeginIndex, last_start + 1):
                            if 0 <= s < len(pX.winRemain):
                                pX.winRemain[s] = True
                        rutilPreFlag = False
                    else:
                        rEndIndex = winEnd
                else:
                    if (sumUtil + sumRutil) >= lminutil:
                        rBeginIndex = winStart
                        rEndIndex = winEnd
                        rutilPreFlag = True

            if trendPreFlag:
                pX.trendPeriod.append(Period(beginIndex, endIndex - 1))
                pX.trendSlope.append(sumSlope / numSliding if numSliding else 0.0)

            if rutilPreFlag:
                pX.rutilPeriod.append(Period(rBeginIndex, rEndIndex - 1))
                last_start = (rEndIndex - 1) - self.numBinOfWin + 1
                for s in range(rBeginIndex, last_start + 1):
                    if 0 <= s < len(pX.winRemain):
                        pX.winRemain[s] = True

            # next outer step (mimic Java for-loop increment)
            winStart += 1
            winEnd = winStart + self.numBinOfWin

    def _cal_slope(self, tul: TrendUtilityList, winStart: int) -> float:
        denom = self.timeDiff[-1] if self.timeDiff else 0.0
        if denom == 0.0:
            return 0.0

        ave = 0.0
        for i in range(winStart, winStart + self.numBinOfWin):
            ave += tul.utilBin[i]
        ave /= float(self.numBinOfWin)

        molecule = 0.0
        j = 0
        for i in range(winStart, winStart + self.numBinOfWin):
            molecule += (tul.utilBin[i] - ave) * self.timeDiff[j]
            j += 1
        return molecule / denom

    def _cal_bin_infos(self, tul: TrendUtilityList) -> None:
        # reset bins (important for constructed lists)
        tul.utilBin = [0] * self.numBin
        tul.rutilBin = [0] * self.numBin

        for e in tul.elements:
            if e.tid < 0 or e.tid >= len(self.binIndexTid):
                continue
            idxBin = self.binIndexTid[e.tid]
            tul.utilBin[idxBin] += e.utils
            tul.rutilBin[idxBin] += e.rutils

    def _write_out(self, prefix: List[int], ult: TrendUtilityList, binlen: int, output_index: bool) -> None:
        self.lthuiCount += 1

        parts = []
        if prefix:
            parts.extend(str(x) for x in prefix)
        parts.append(str(ult.item))

        s = " ".join(parts) + " #PERIOD-UTIL-SLOPE"

        for i, per in enumerate(ult.trendPeriod):
            if output_index:
                s += f" [{per.beginIndex + 1},{per.endIndex + 1}] "
            else:
                start_ts = self.minTime + per.beginIndex * binlen
                end_ts = self.minTime + (per.endIndex + 1) * binlen - 1
                s += f" [{start_ts},{end_ts}] "

            slope = ult.trendSlope[i] if i < len(ult.trendSlope) else 0.0
            total = 0
            for b in range(per.beginIndex, per.endIndex + 1):
                total += ult.utilBin[b]
            s += f"({total},{slope}) "

        self._out.write(s.rstrip() + "\n")


# -----------------------------
# Main (Python equivalent of MainTestLTHUIMiner.java)
# -----------------------------
def main():
    lminutil = 90
    winlen = 9
    binlen = 3
    minslope = 5.0
    database_start_timestamp = -1
    output_index = False

    input_file = "Java//src//DB_LTHUI.txt"
    output_file = "Java//src//output_py.txt"

    algo = AlgoLTHUIMiner()
    algo.run_algorithm(
        input_file=input_file,
        output_file=output_file,
        lminutil=lminutil,
        winlen=winlen,
        binlen=binlen,
        minslope=minslope,
        database_start_timestamp=database_start_timestamp,
        output_index=output_index
    )
    algo.print_stats()
    print(f"Done. Wrote: {output_file}")


if __name__ == "__main__":
    main()