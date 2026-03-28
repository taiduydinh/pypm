#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHUI-Miner(MAX) (E-CHUI-Mine(Max) with optional EUCP)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import argparse
import time


# =========================
# MemoryLogger (same as SPMF)
# =========================
class MemoryLogger:
    _instance = None

    def __init__(self) -> None:
        self.maxMemoryMB: float = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self.maxMemoryMB = 0.0

    def checkMemory(self) -> float:
        # Portable-ish approximation without external libs:
        # We keep it as 0.0 if we cannot measure reliably.
        # (SPMF uses JVM memory; Python equivalent is not 1:1.)
        current = 0.0
        if current > self.maxMemoryMB:
            self.maxMemoryMB = current
        return current

    def getMaxMemory(self) -> float:
        return self.maxMemoryMB


# =========================
# Core structures
# =========================
@dataclass
class Element:
    tid: int
    iutils: int
    rutils: int


class UtilityList:
    def __init__(self, item: int) -> None:
        self.item: int = item
        self.sumIutils: int = 0
        self.sumRutils: int = 0
        self.elements: List[Element] = []  # elements are appended by increasing tid

    def addElement(self, element: Element) -> None:
        self.sumIutils += element.iutils
        self.sumRutils += element.rutils
        self.elements.append(element)

    def getSupport(self) -> int:
        return len(self.elements)

    def getUtils(self) -> int:
        return self.sumIutils


@dataclass
class PairItemUtility:
    item: int = 0
    utility: int = 0

    def __str__(self) -> str:
        return f"[{self.item},{self.utility}]"


@dataclass
class Itemset:
    items: List[int]
    utility: int
    support: int


# =========================
# AlgoCHUIMinerMax
# =========================
class AlgoCHUIMinerMax:
    def __init__(self, useEUCPstrategy: bool = True) -> None:
        self.useEUCPstrategy = useEUCPstrategy

        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0

        self.mhuiCount: int = 0
        self.candidateCount: int = 0

        self.mapItemToTWU: Dict[int, int] = {}
        self.mapFMAP: Optional[Dict[int, Dict[int, int]]] = None

        self.writer = None  # file handle
        self.mhuis: Optional[List[Itemset]] = None

        self.minUtility: int = 0

        # MID-list machinery (specific to MHUI / CHUI-Miner(MAX))
        self.nextMID: int = 0
        self.mapItemToMIDs: Dict[int, List[int]] = {}

    # -------- MID lists ----------
    def getMIDList(self, item: int) -> List[int]:
        return self.mapItemToMIDs[item]

    def addMIDtoMIDListOfItem(self, mid: int, item: int) -> None:
        self.getMIDList(item).append(mid)

    def intersectTwoMIDLists(self, midlist1: List[int], midlist2: List[int]) -> List[int]:
        # Java code uses a naive HashSet intersection; order does not matter.
        return list(set(midlist1).intersection(midlist2))

    # -------- Comparison ----------
    def compareItems(self, item1: int, item2: int) -> int:
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else compare

    # -------- EUCP pruning ----------
    def checkEUCPStrategy(self, itemX: int, itemY: int) -> bool:
        # Java MAX version prunes only if twuF exists AND twuF < minUtility
        if not self.mapFMAP:
            return False
        mapTWUF = self.mapFMAP.get(itemX)
        if mapTWUF is not None:
            twuF = mapTWUF.get(itemY)
            if twuF is not None and twuF < self.minUtility:
                return True
        return False

    # -------- Utility-list helpers ----------
    @staticmethod
    def _findElementWithTID(ulist: UtilityList, tid: int) -> Optional[Element]:
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

    def containsAllTIDS(self, ul1: UtilityList, ul2: UtilityList) -> bool:
        # ul1 contains all tids of ul2?
        for elmX in ul2.elements:
            if self._findElementWithTID(ul1, elmX.tid) is None:
                return False
        return True

    def isPassingHUIPruning(self, ulist: UtilityList) -> bool:
        return (ulist.sumIutils + ulist.sumRutils) >= self.minUtility

    def is_dup(self, newgenTIDs: UtilityList, preset: List[UtilityList]) -> bool:
        # If tidset(newgen) is included in tidset(j) for some j in preset => duplicate
        for j in preset:
            containsAll = True
            for elmX in newgenTIDs.elements:
                if self._findElementWithTID(j, elmX.tid) is None:
                    containsAll = False
                    break
            if containsAll:
                return True
        return False

    @staticmethod
    def appendItem(itemset: List[int], item: int) -> List[int]:
        return itemset + [item]

    def construct(self, uX: UtilityList, uE: UtilityList) -> UtilityList:
        # Construct utility list of Xe
        uXE = UtilityList(uE.item)
        for elmX in uX.elements:
            elmE = self._findElementWithTID(uE, elmX.tid)
            if elmE is None:
                continue
            # IMPORTANT: rutils subtraction as in Java: elmX.rutils - elmE.iutils
            elmXe = Element(elmX.tid, elmX.iutils + elmE.iutils, elmX.rutils - elmE.iutils)
            uXE.addElement(elmXe)
        return uXE

    # -------- Output ----------
    def saveMHUI(self, itemset: List[int], utility: int, support: int) -> None:
        self.mhuiCount += 1
        if self.writer is None:
            assert self.mhuis is not None
            self.mhuis.append(Itemset(itemset=list(itemset), utility=int(utility), support=int(support)))
        else:
            # match Java output formatting
            buf = []
            for it in itemset:
                buf.append(str(it))
                buf.append(" ")
            buf.append(" #SUP: ")
            buf.append(str(support))
            buf.append(" #UTIL: ")
            buf.append(str(utility))
            self.writer.write("".join(buf).rstrip() + "\n")

    # -------- Core recursion ----------
    def chuimineMAX_eucp(
        self,
        firstTime: bool,
        closedSet: List[int],
        closedsetMIDs: List[int],
        closedSetUL: Optional[UtilityList],
        preset: List[UtilityList],
        postset: List[UtilityList],
    ) -> bool:
        foundOneMHUI = False

        for iUL in postset:
            # L4: newgen_TIDs and newgen_MIDs
            if firstTime:
                newgen_TIDs = iUL
                newgen_MIDs = self.getMIDList(iUL.item)
            else:
                assert closedSetUL is not None
                newgen_TIDs = self.construct(closedSetUL, iUL)
                newgen_MIDs = self.intersectTwoMIDLists(closedsetMIDs, self.getMIDList(iUL.item))

            if self.isPassingHUIPruning(newgen_TIDs):
                # newGen = closedSet U {i}
                newGen = self.appendItem(closedSet, iUL.item)

                if not self.is_dup(newgen_TIDs, preset):
                    closedSetNew = list(newGen)
                    closedsetNewTIDs = newgen_TIDs
                    closedsetNewMIDs = list(newgen_MIDs)

                    postsetNew: List[UtilityList] = []
                    passedHUIPruning = True

                    # for each j in postset
                    for jUL in postset:
                        if jUL.item == iUL.item or self.compareItems(jUL.item, iUL.item) < 0:
                            continue

                        # EUCP pruning
                        if self.useEUCPstrategy and self.checkEUCPStrategy(iUL.item, jUL.item):
                            continue

                        self.candidateCount += 1

                        if self.containsAllTIDS(jUL, newgen_TIDs):
                            closedSetNew = self.appendItem(closedSetNew, jUL.item)
                            closedsetNewTIDs = self.construct(closedsetNewTIDs, jUL)
                            closedsetNewMIDs = self.intersectTwoMIDLists(closedsetNewMIDs, self.getMIDList(jUL.item))

                            if not self.isPassingHUIPruning(closedsetNewTIDs):
                                passedHUIPruning = False
                                break
                        else:
                            postsetNew.append(jUL)

                    if passedHUIPruning:
                        # MID-list pruning with Z = ClosedSetNew U Postset(ClosedSetNew)
                        # If MIDLIST(Z) not empty -> prune remaining exploration for this branch (break outer loop)
                        if len(postsetNew) > 0:
                            zMIDs = self.intersectTwoMIDLists(closedsetNewMIDs, self.getMIDList(postsetNew[0].item))
                            idx = 1
                            while idx < len(postsetNew) and len(zMIDs) != 0:
                                zMIDs = self.intersectTwoMIDLists(zMIDs, self.getMIDList(postsetNew[idx].item))
                                idx += 1
                            if len(zMIDs) > 0:
                                # In Java this is `break;` from the for(iUL in postset)
                                break

                        presetNew = list(preset)
                        hasSupersetMHUI = self.chuimineMAX_eucp(
                            False,
                            closedSetNew,
                            closedsetNewMIDs,
                            closedsetNewTIDs,
                            presetNew,
                            postsetNew,
                        )

                        # CHECK IF CLOSEDSETNEW IS MHUI:
                        # - no succeeding supersets that are MHUIs
                        # - empty MID list
                        # - is HUI
                        if (not hasSupersetMHUI) and (len(closedsetNewMIDs) == 0) and (closedsetNewTIDs.sumIutils >= self.minUtility):
                            foundOneMHUI = True
                            mid = self.nextMID
                            self.nextMID += 1
                            for it in closedSetNew:
                                self.addMIDtoMIDListOfItem(mid, it)
                            self.saveMHUI(closedSetNew, closedsetNewTIDs.sumIutils, len(closedsetNewTIDs.elements))

                        foundOneMHUI = foundOneMHUI or hasSupersetMHUI

                    preset.append(iUL)

        return foundOneMHUI

    # -------- Main entry ----------
    def runAlgorithm(self, inputPath: str, minUtility: int, outputPath: Optional[str]) -> Optional[List[Itemset]]:
        MemoryLogger.getInstance().reset()
        self.minUtility = int(minUtility)

        # MID initialization
        self.nextMID = 0
        self.mapItemToMIDs = {}

        # writer or memory
        self.writer = open(outputPath, "w", encoding="utf-8") if outputPath else None
        self.mhuis = [] if outputPath is None else None

        # EUCP structure
        self.mapFMAP = {} if self.useEUCPstrategy else None

        self.startTimestamp = time.time()

        # 1st DB scan: TWU
        self.mapItemToTWU = {}
        with open(inputPath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ("#", "%", "@"):
                    continue
                split = line.split(":")
                items = split[0].strip().split()
                tu = int(split[1].strip())
                for s in items:
                    item = int(s)
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + tu

        # create utility lists for promising items
        listOfUtilityLists: List[UtilityList] = []
        mapItemToUtilityList: Dict[int, UtilityList] = {}
        for item, twu in self.mapItemToTWU.items():
            if twu >= self.minUtility:
                ul = UtilityList(item)
                mapItemToUtilityList[item] = ul
                listOfUtilityLists.append(ul)

        # sort by increasing TWU (then lexical)
        listOfUtilityLists.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        # 2nd DB scan: build utility lists + EUCP maps
        tid = 0
        with open(inputPath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ("#", "%", "@"):
                    continue
                split = line.split(":")
                items = split[0].strip().split()
                utils = split[2].strip().split()

                revised: List[PairItemUtility] = []
                newTU = 0
                for i in range(len(items)):
                    it = int(items[i])
                    ut = int(utils[i])
                    if self.mapItemToTWU.get(it, 0) >= self.minUtility:
                        revised.append(PairItemUtility(it, ut))
                        newTU += ut  # NEW OPTIMIZATION (same as Java)

                # sort revised transaction by compareItems
                revised.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                remainingUtility = newTU
                for i, pair in enumerate(revised):
                    remainingUtility -= pair.utility
                    ul_item = mapItemToUtilityList[pair.item]
                    ul_item.addElement(Element(tid, pair.utility, remainingUtility))

                    # EUCP updates
                    if self.useEUCPstrategy:
                        assert self.mapFMAP is not None
                        mapFMAPItem = self.mapFMAP.get(pair.item)
                        if mapFMAPItem is None:
                            mapFMAPItem = {}
                            self.mapFMAP[pair.item] = mapFMAPItem
                        for j in range(i + 1, len(revised)):
                            after = revised[j]
                            prev = mapFMAPItem.get(after.item)
                            if prev is None:
                                mapFMAPItem[after.item] = newTU
                            else:
                                mapFMAPItem[after.item] = prev + newTU

                tid += 1

        # init MID lists of single items
        for ul in listOfUtilityLists:
            self.mapItemToMIDs[ul.item] = []

        MemoryLogger.getInstance().checkMemory()

        # mine recursively
        self.chuimineMAX_eucp(True, [], [], None, [], listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        if self.writer is not None:
            self.writer.close()
            self.writer = None

        self.endTimestamp = time.time()
        return self.mhuis

    def printStats(self) -> None:
        title = "CHUIMine(max)_EUCP" if self.useEUCPstrategy else "CHUIMine(max)"
        print(f"=============  {title} ALGORITHM - STATS =============")
        print(f" Total time ~ {int((self.endTimestamp - self.startTimestamp) * 1000)} ms")
        print(f" Memory ~ {MemoryLogger.getInstance().getMaxMemory():.3f} MB")
        print(f" MHUI count : {self.mhuiCount}")
        print(f" Candidate count : {self.candidateCount}")
        print("=====================================================")


# =========================
# Main (direct parameters, no argparse)
# =========================

# ====== SET PARAMETERS HERE ======
INPUT_PATH = "DB_Utility.txt"
MIN_UTILITY = 20
OUTPUT_PATH = "output_py.txt"   # use None to keep results in memory
USE_EUCP = True
# =================================


def main() -> None:
    algo = AlgoCHUIMinerMax(useEUCPstrategy=USE_EUCP)

    mhuis = algo.runAlgorithm(
        inputPath=INPUT_PATH,
        minUtility=MIN_UTILITY,
        outputPath=OUTPUT_PATH,
    )

    algo.printStats()

    # Optional: print results if OUTPUT_PATH is None
    if OUTPUT_PATH is None and mhuis is not None:
        for itemset in mhuis:
            print(
                f"{' '.join(map(str, itemset.items))} "
                f"#SUP: {itemset.support} #UTIL: {itemset.utility}"
            )


if __name__ == "__main__":
    main()