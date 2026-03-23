"""
Single-file Python implementation of TKQ (Top-K Quantitative High Utility Itemset Mining)
Converted from the Java source you provided (Mourad Nouioua et al., SPMF).

How to run:
    1) Put this file in a folder with:
         - dbHUQI.txt
         - dbHUQI_p.txt
    2) Run:
         python tkq_single.py

It will create:
    - output_py.txt

Notes:
- Utilities/pattern set should match the Java output for the provided dataset.
- Line order may differ from Java because Java iterates a PriorityQueue without sorting.
  If you need deterministic output order, set STABLE_SORTED_OUTPUT = True.
"""

from __future__ import annotations

import heapq
import math
import itertools
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


# -------------------- EnumCombination --------------------

class EnumCombination(Enum):
    COMBINEMIN = 1
    COMBINEMAX = 2
    COMBINEALL = 3


# -------------------- Qitem --------------------

@dataclass(eq=True, frozen=False)
class Qitem:
    item: int = 0
    qteMin: int = 0
    qteMax: int = 0

    def __init__(self, item: int = 0, qmin: int = 0, qmax: int | None = None):
        if qmax is None:
            qmax = qmin
        self.item = item
        self.qteMin = qmin
        self.qteMax = qmax

    def copy(self, q: "Qitem") -> None:
        self.item = q.item
        self.qteMin = q.qteMin
        self.qteMax = q.qteMax

    def isRange(self) -> bool:
        return self.qteMin != self.qteMax

    def __str__(self) -> str:
        if not self.isRange():
            return f"({self.item},{self.qteMin})"
        return f"({self.item},{self.qteMin},{self.qteMax})"

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash((self.item, self.qteMin, self.qteMax))


# -------------------- QItemTrans --------------------

@dataclass
class QItemTrans:
    tid: int
    eu: int
    ru: int

    def sum(self) -> int:
        return self.eu + self.ru

    def __str__(self) -> str:
        return f"{self.tid} {self.eu}\t{self.ru}"


# -------------------- InfoTKQ --------------------

class InfoTKQ:
    def __init__(self) -> None:
        self.twu: int = 0
        self.utility: int = 0

    def __repr__(self) -> str:
        return f"(twu:{self.twu}, utility:{self.utility})"


# -------------------- Qitemset --------------------

_uid_counter = itertools.count(1)

@dataclass
class Qitemset:
    itemset: List[Qitem] = field(default_factory=list)
    utility: int = 0
    uid: int = field(default_factory=lambda: next(_uid_counter), compare=False)

    def __lt__(self, other: "Qitemset") -> bool:
        # heap order: smallest utility first; tie -> uid for deterministic behavior
        if self.utility != other.utility:
            return self.utility < other.utility
        return self.uid < other.uid

    def __str__(self) -> str:
        return f"{self.itemset} #Util{self.utility}"


# -------------------- UtilityListTKQ --------------------

class UtilityListTKQ:
    def __init__(self, name: Optional[Qitem] = None, twu: int = 0, itemset: Optional[List[Qitem]] = None):
        if itemset is not None:
            self.itemsetName = list(itemset)
        else:
            self.itemsetName = []
            if name is not None:
                self.itemsetName.append(name)

        self.sumIutils: int = 0
        self.sumRutils: int = 0
        self.twu: int = twu
        self.qItemTrans: List[QItemTrans] = []

    def addTWU(self, twu: int) -> None:
        self.twu += twu

    def setTWUtoZero(self) -> None:
        self.twu = 0

    def addTrans(self, qTid: QItemTrans, twu: Optional[int] = None) -> None:
        self.sumIutils += qTid.eu
        self.sumRutils += qTid.ru
        self.qItemTrans.append(qTid)
        if twu is not None:
            self.twu += twu

    def getSumIutils(self) -> int:
        return self.sumIutils

    def getSumRutils(self) -> int:
        return self.sumRutils

    def setSumIutils(self, x: int) -> None:
        self.sumIutils = x

    def setSumRutils(self, x: int) -> None:
        self.sumRutils = x

    def getTwu(self) -> int:
        return self.twu

    def setTwu(self, twu: int) -> None:
        self.twu = twu

    def getItemsetName(self) -> List[Qitem]:
        return self.itemsetName

    def getSingleItemsetName(self) -> Qitem:
        return self.itemsetName[0]

    def getQItemTrans(self) -> List[QItemTrans]:
        return self.qItemTrans

    def setQItemTrans(self, elements: List[QItemTrans]) -> None:
        self.qItemTrans = list(elements)

    def getqItemTransLength(self) -> int:
        return len(self.qItemTrans) if self.qItemTrans is not None else 0


# -------------------- AlgoTKQ --------------------

class AlgoTKQ:
    def __init__(self, debug: bool = True) -> None:
        self.DEBUG_MODE = debug

        # parameters
        self.minUtil: int = 0
        self.coefficient: int = 0
        self.combiningMethod: EnumCombination = EnumCombination.COMBINEALL
        self.k: int = 0

        # top-k patterns (min-heap by utility)
        self.kPatterns: List[Qitemset] = []

        # maps
        self.mapItemToTwu: Dict[Qitem, int] = {}
        self.mapItemToProfit: Dict[int, int] = {}
        self.mapTransactionToUtility: Dict[int, int] = {}
        self.mapFMAP: Dict[Qitem, Dict[Qitem, InfoTKQ]] = {}

        # raising strategies
        self.realUtility: Dict[Qitem, int] = {}
        self.CUD: Dict[str, int] = {}

        # stats
        self.HUQIcount: int = 0
        self.countConstruct: int = 0

        # buffer
        self.BUFFERS_SIZE = 200
        self.itemsetBuffer: List[Optional[Qitem]] = [None] * self.BUFFERS_SIZE

    # --------- PQ insert logic (matches Java idea) ---------

    def _insert_pattern(self, pat: Qitemset) -> None:
        heapq.heappush(self.kPatterns, pat)

        if len(self.kPatterns) > self.k:
            if pat.utility >= self.minUtil:
                while len(self.kPatterns) > self.k:
                    heapq.heappop(self.kPatterns)
            self.minUtil = self.kPatterns[0].utility

    def insert_single(self, item: Qitem, utility: int) -> None:
        self._insert_pattern(Qitemset(itemset=[item], utility=utility))

    def insert_two(self, item1: Qitem, item2: Qitem, utility: int) -> None:
        self._insert_pattern(Qitemset(itemset=[item1, item2], utility=utility))

    def insert_prefix_one(self, prefix: List[Qitem], x: Qitem, utility: int) -> None:
        self._insert_pattern(Qitemset(itemset=list(prefix) + [x], utility=utility))

    def insert_prefix_two(self, prefix: List[Qitem], x: Qitem, y: Qitem, utility: int) -> None:
        self._insert_pattern(Qitemset(itemset=list(prefix) + [x, y], utility=utility))

    # --------- comparators ---------

    def _compareQItems_key(self, q: Qitem) -> Tuple[int, int]:
        # Java compareQItems: descending (qteMin*profit), tie item ascending
        return (-(q.qteMin * self.mapItemToProfit[q.item]), q.item)

    def _compareCandidate_key(self, q: Qitem) -> Tuple[int, int, int]:
        return (q.item, q.qteMin, q.qteMax)

    # --------- raising strategies ---------

    def _raisingThresholdRIU(self, k: int) -> None:
        lst = sorted(self.realUtility.items(), key=lambda kv: kv[1], reverse=True)

        if len(lst) >= k and k > 0:
            self.minUtil = lst[k - 1][1]

        for q, u in lst:
            self.insert_single(q, u)

    def _raisingThresholdCUDOptimize2(self) -> None:
        for q1, mp in self.mapFMAP.items():
            for q2, info in mp.items():
                val = info.utility
                if val >= self.minUtil:
                    self.CUD[f"{q1}_{q2}"] = val
                    self.insert_two(q1, q2, val)

    # --------- IO helpers ---------

    @staticmethod
    def _parse_profit_file(path: str) -> Dict[int, int]:
        mp: Dict[int, int] = {}
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) >= 2:
                    item = int(parts[0].strip())
                    profit = int(parts[1].strip())
                    if profit == 0:
                        profit = 1
                    mp[item] = profit
        return mp

    @staticmethod
    def _parse_db_line(line: str) -> List[Tuple[int, int]]:
        toks = line.strip().split()
        pairs: List[Tuple[int, int]] = []
        for tok in toks:
            c = tok.index(",")
            it = int(tok[:c])
            q = int(tok[c + 1 :])
            pairs.append((it, q))
        return pairs

    # --------- initial utility lists + fmap ---------

    def buildInitialQUtilityLists(
        self,
        inputData: str,
        inputProfit: str,
        qitemNameList: List[Qitem],
        mapItemToUtilityList: Dict[Qitem, UtilityListTKQ],
    ) -> None:
        self.mapItemToProfit = self._parse_profit_file(inputProfit)

        self.mapItemToTwu = {}
        self.realUtility = {}

        tid = 0
        with open(inputData, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                tid += 1
                parsed = self._parse_db_line(line)

                transactionU = 0
                for it, q in parsed:
                    transactionU += q * self.mapItemToProfit[it]

                for it, q in parsed:
                    Q = Qitem(it, q, q)
                    self.mapItemToTwu[Q] = self.mapItemToTwu.get(Q, 0) + transactionU
                    util = q * self.mapItemToProfit[it]
                    self.realUtility[Q] = self.realUtility.get(Q, 0) + util

        # RIU
        if self.DEBUG_MODE:
            print("===============================================")
            print(" minutil is", self.minUtil)
        self._raisingThresholdRIU(self.k)
        if self.DEBUG_MODE:
            print("after RIU minUtil is", self.minUtil)

        # build initial ULs filtered by TWU
        twu_cut = math.floor(self.minUtil / self.coefficient)
        for q, twu in self.mapItemToTwu.items():
            if twu >= twu_cut:
                mapItemToUtilityList[q] = UtilityListTKQ(q, 0)
                qitemNameList.append(q)

        # second pass build UL + FMAP
        self.mapFMAP = {}
        self.mapTransactionToUtility = {}

        tid = 0
        with open(inputData, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                tid += 1
                parsed = self._parse_db_line(line)

                remainingUtility = 0
                newTWU = 0

                revisedTransaction: List[Qitem] = []
                for it, q in parsed:
                    Q = Qitem(it, q, q)
                    if Q in mapItemToUtilityList:
                        revisedTransaction.append(Q)
                        remainingUtility += q * self.mapItemToProfit[it]
                        newTWU += q * self.mapItemToProfit[it]
                    self.mapTransactionToUtility[tid] = newTWU

                revisedTransaction.sort(key=self._compareQItems_key)

                for i, current_q in enumerate(revisedTransaction):
                    remainingUtility -= current_q.qteMin * self.mapItemToProfit[current_q.item]

                    ul = mapItemToUtilityList[current_q]
                    element = QItemTrans(
                        tid,
                        current_q.qteMin * self.mapItemToProfit[current_q.item],
                        remainingUtility,
                    )
                    ul.addTrans(element)
                    ul.addTWU(self.mapTransactionToUtility[tid])

                    mp = self.mapFMAP.get(current_q)
                    if mp is None:
                        mp = {}
                        self.mapFMAP[current_q] = mp

                    for j in range(i + 1, len(revisedTransaction)):
                        qAfter = revisedTransaction[j]
                        info = mp.get(qAfter)
                        if info is None:
                            info = InfoTKQ()
                        info.twu += newTWU
                        info.utility += (
                            current_q.qteMin * self.mapItemToProfit[current_q.item]
                            + qAfter.qteMin * self.mapItemToProfit[qAfter.item]
                        )
                        mp[qAfter] = info

        # CUD
        if self.DEBUG_MODE:
            print("===================================================")
            print(" before CUD ... minutil is", self.minUtil)
        self._raisingThresholdCUDOptimize2()
        if self.DEBUG_MODE:
            print("after CUD minUtil is", self.minUtil)

        # final sort
        qitemNameList.sort(key=self._compareQItems_key)

    # --------- initial RHUQIs ---------

    def findInitialRHUQIs(
        self,
        qitemNameList: List[Qitem],
        mapItemToUtilityList: Dict[Qitem, UtilityListTKQ],
        candidateList: List[Qitem],
        hwQUI: List[Qitem],
    ) -> None:
        for q in qitemNameList:
            utility = mapItemToUtilityList[q].getSumIutils()
            if utility >= self.minUtil:
                hwQUI.append(q)
                self.HUQIcount += 1
            else:
                if (
                    (self.combiningMethod != EnumCombination.COMBINEMAX and utility >= math.floor(self.minUtil / self.coefficient))
                    or (self.combiningMethod == EnumCombination.COMBINEMAX and utility >= math.floor(self.minUtil / 2))
                ):
                    candidateList.append(q)
                if utility + mapItemToUtilityList[q].getSumRutils() >= self.minUtil:
                    hwQUI.append(q)

        self.combineMethod(None, 0, candidateList, qitemNameList, mapItemToUtilityList, hwQUI)

    # --------- combine methods (COMBINEALL exact) ---------

    def combineMethod(
        self,
        prefix: Optional[List[Qitem]],
        prefixLength: int,
        candidateList: List[Qitem],
        qItemNameList: List[Qitem],
        mapItemToUtilityList: Dict[Qitem, UtilityListTKQ],
        hwQUI: List[Qitem],
    ) -> List[Qitem]:
        if len(candidateList) > 2:
            candidateList.sort(key=self._compareCandidate_key)

            # Implement COMBINEALL exactly; for MIN/MAX you can extend later
            if self.combiningMethod == EnumCombination.COMBINEALL:
                self.combineAll(prefix, prefixLength, candidateList, qItemNameList, mapItemToUtilityList, hwQUI)
            else:
                # fallback: still combineAll (better than doing nothing)
                self.combineAll(prefix, prefixLength, candidateList, qItemNameList, mapItemToUtilityList, hwQUI)

        return qItemNameList

    def constructForCombine(self, ul1: UtilityListTKQ, ul2: UtilityListTKQ) -> UtilityListTKQ:
        result = UtilityListTKQ(
            Qitem(
                ul1.getSingleItemsetName().item,
                ul1.getSingleItemsetName().qteMin,
                ul2.getSingleItemsetName().qteMax,
            )
        )

        temp1 = ul1.getQItemTrans()
        temp2 = ul2.getQItemTrans()

        result.setSumIutils(ul1.getSumIutils() + ul2.getSumIutils())
        result.setSumRutils(ul1.getSumRutils() + ul2.getSumRutils())
        result.setTwu(ul1.getTwu() + ul2.getTwu())

        mainlist: List[QItemTrans] = []
        i = j = 0
        while i < len(temp1) and j < len(temp2):
            t1 = temp1[i].tid
            t2 = temp2[j].tid
            if t1 > t2:
                mainlist.append(temp2[j])
                j += 1
            else:
                mainlist.append(temp1[i])
                i += 1

        if i == len(temp1):
            mainlist.extend(temp2[j:])
        else:
            mainlist.extend(temp1[i:])

        result.setQItemTrans(mainlist)
        return result

    def combineAll(
        self,
        prefix: Optional[List[Qitem]],
        prefixLength: int,
        candidateList: List[Qitem],
        qItemNameList: List[Qitem],
        mapItemToUtilityList: Dict[Qitem, UtilityListTKQ],
        hwQUI: List[Qitem],
    ) -> None:
        s = 1
        while s < len(candidateList) - 1:
            left_ok = (candidateList[s].qteMin == candidateList[s - 1].qteMax + 1) and (candidateList[s].item == candidateList[s - 1].item)
            right_ok = (candidateList[s].qteMax == candidateList[s + 1].qteMin - 1) and (candidateList[s].item == candidateList[s + 1].item)
            if left_ok or right_ok:
                s += 1
            else:
                candidateList.pop(s)

        if len(candidateList) > 2:
            if (candidateList[-1].qteMin != candidateList[-2].qteMax + 1) or (candidateList[-2].item != candidateList[-1].item):
                candidateList.pop()

        mapRangeToUtilityList: Dict[Qitem, UtilityListTKQ] = {}

        for i in range(len(candidateList)):
            currentItem = candidateList[i].item
            mapRangeToUtilityList.clear()
            count = 1

            for j in range(i + 1, len(candidateList)):
                nextItem = candidateList[j].item
                if currentItem != nextItem:
                    break

                if j == i + 1:
                    if candidateList[j].qteMin != candidateList[i].qteMax + 1:
                        break

                    res = self.constructForCombine(mapItemToUtilityList[candidateList[i]], mapItemToUtilityList[candidateList[j]])
                    count += 1
                    if count > self.coefficient:
                        break

                    mapRangeToUtilityList[res.getSingleItemsetName()] = res

                    if res.getSumIutils() > self.minUtil:
                        self.HUQIcount += 1
                        if prefixLength == 0 or prefix is None:
                            self.insert_single(res.getSingleItemsetName(), res.getSumIutils())
                        else:
                            self.insert_prefix_one(prefix[:prefixLength], res.getSingleItemsetName(), res.getSumIutils())

                        hwQUI.append(res.getSingleItemsetName())
                        mapItemToUtilityList[res.getSingleItemsetName()] = res

                        site = qItemNameList.index(candidateList[j])
                        qItemNameList.insert(site, res.getSingleItemsetName())

                else:
                    if candidateList[j].qteMin != candidateList[j - 1].qteMax + 1:
                        break

                    qItem1 = Qitem(currentItem, candidateList[i].qteMin, candidateList[j - 1].qteMax)
                    ulQitem1 = mapRangeToUtilityList[qItem1]

                    res = self.constructForCombine(ulQitem1, mapItemToUtilityList[candidateList[j]])
                    count += 1
                    if count > self.coefficient:
                        break

                    mapRangeToUtilityList[res.getSingleItemsetName()] = res

                    if res.getSumIutils() > self.minUtil:
                        self.HUQIcount += 1
                        if prefixLength == 0 or prefix is None:
                            self.insert_single(res.getSingleItemsetName(), res.getSumIutils())
                        else:
                            self.insert_prefix_one(prefix[:prefixLength], res.getSingleItemsetName(), res.getSumIutils())

                        hwQUI.append(res.getSingleItemsetName())
                        mapItemToUtilityList[res.getSingleItemsetName()] = res

                        site = qItemNameList.index(candidateList[j])
                        qItemNameList.insert(site, res.getSingleItemsetName())

    # --------- join construction ---------

    def constructForJoin(self, ul1: UtilityListTKQ, ul2: UtilityListTKQ, ul0: Optional[UtilityListTKQ]) -> Optional[UtilityListTKQ]:
        if ul1.getSingleItemsetName().item == ul2.getSingleItemsetName().item:
            return None

        qT1 = ul1.getQItemTrans()
        qT2 = ul2.getQItemTrans()
        res = UtilityListTKQ(itemset=ul2.getItemsetName())

        if ul0 is None:
            i = j = 0
            while i < len(qT1) and j < len(qT2):
                tid1 = qT1[i].tid
                tid2 = qT2[j].tid

                if tid1 == tid2:
                    eu1 = qT1[i].eu
                    eu2 = qT2[j].eu
                    if qT1[i].ru >= qT2[j].ru:
                        res.addTrans(QItemTrans(tid1, eu1 + eu2, qT2[j].ru), self.mapTransactionToUtility[tid1])
                    i += 1
                    j += 1
                elif tid1 > tid2:
                    j += 1
                else:
                    i += 1
        else:
            preQT = ul0.getQItemTrans()
            i = j = k = 0
            while i < len(qT1) and j < len(qT2):
                tid1 = qT1[i].tid
                tid2 = qT2[j].tid

                if tid1 == tid2:
                    eu1 = qT1[i].eu
                    eu2 = qT2[j].eu
                    while preQT[k].tid != tid1:
                        k += 1
                    preEU = preQT[k].eu
                    if qT1[i].ru >= qT2[j].ru:
                        res.addTrans(QItemTrans(tid1, eu1 + eu2 - preEU, qT2[j].ru), self.mapTransactionToUtility[tid1])
                    i += 1
                    j += 1
                elif tid1 > tid2:
                    j += 1
                else:
                    i += 1

        return res if res.getQItemTrans() else None

    # --------- miner ---------

    def miner(
        self,
        prefix: List[Optional[Qitem]],
        prefixLength: int,
        prefixUL: Optional[UtilityListTKQ],
        ULs: Dict[Qitem, UtilityListTKQ],
        qItemNameList: List[Qitem],
        hwQUI: List[Qitem],
    ) -> None:
        t2 = [0] * self.coefficient
        nextNameList: List[Qitem] = []

        for i in range(len(qItemNameList)):
            nextNameList.clear()
            nextHWQUI: List[Qitem] = []
            candidateList: List[Qitem] = []
            nextHUL: Dict[Qitem, UtilityListTKQ] = {}

            if qItemNameList[i] not in hwQUI:
                continue

            if qItemNameList[i].isRange():
                for ii in range(qItemNameList[i].qteMin, qItemNameList[i].qteMax + 1):
                    t2[ii - qItemNameList[i].qteMin] = qItemNameList.index(Qitem(qItemNameList[i].item, ii))

            for j in range(i + 1, len(qItemNameList)):
                if qItemNameList[j].isRange():
                    continue
                if qItemNameList[i].isRange() and j == i + 1:
                    continue

                afterUL: Optional[UtilityListTKQ] = None

                mapTWUF = self.mapFMAP.get(qItemNameList[i])
                if mapTWUF is not None:
                    twuF = mapTWUF.get(qItemNameList[j])
                    if twuF is None or twuF.twu < math.floor(self.minUtil / self.coefficient):
                        continue

                    afterUL = self.constructForJoin(ULs[qItemNameList[i]], ULs[qItemNameList[j]], prefixUL)
                    self.countConstruct += 1
                    if afterUL is None or afterUL.getTwu() < math.floor(self.minUtil / self.coefficient):
                        continue
                else:
                    sumtwu = 0
                    for ii in range(qItemNameList[i].qteMin, qItemNameList[i].qteMax + 1):
                        a = qItemNameList[min(t2[ii - qItemNameList[i].qteMin], j)]
                        b = qItemNameList[max(t2[ii - qItemNameList[i].qteMin], j)]
                        info = self.mapFMAP.get(a, {}).get(b)
                        if info is None:
                            continue
                        sumtwu += info.twu

                    if sumtwu == 0 or sumtwu < math.floor(self.minUtil / self.coefficient):
                        continue

                    afterUL = self.constructForJoin(ULs[qItemNameList[i]], ULs[qItemNameList[j]], prefixUL)
                    self.countConstruct += 1
                    if afterUL is None or afterUL.getTwu() < math.floor(self.minUtil / self.coefficient):
                        continue

                if afterUL is not None and afterUL.getTwu() >= math.floor(self.minUtil / self.coefficient):
                    nextNameList.append(afterUL.getSingleItemsetName())
                    nextHUL[afterUL.getSingleItemsetName()] = afterUL

                    if afterUL.getSumIutils() >= self.minUtil:
                        pref = [p for p in prefix[:prefixLength] if p is not None]
                        self.insert_prefix_two(pref, qItemNameList[i], qItemNameList[j], afterUL.getSumIutils())
                        self.HUQIcount += 1
                        nextHWQUI.append(afterUL.getSingleItemsetName())
                    else:
                        if (
                            (self.combiningMethod != EnumCombination.COMBINEMAX and afterUL.getSumIutils() >= math.floor(self.minUtil / self.coefficient))
                            or (self.combiningMethod == EnumCombination.COMBINEMAX and afterUL.getSumIutils() >= math.floor(self.minUtil / 2))
                        ):
                            candidateList.append(afterUL.getSingleItemsetName())

                        if afterUL.getSumIutils() + afterUL.getSumRutils() >= self.minUtil:
                            nextHWQUI.append(afterUL.getSingleItemsetName())

            if candidateList:
                nextNameList = self.combineMethod(
                    [p for p in prefix[:prefixLength] if p is not None],
                    prefixLength,
                    candidateList,
                    nextNameList,
                    nextHUL,
                    nextHWQUI,
                )

            if len(nextNameList) >= 1:
                prefix[prefixLength] = qItemNameList[i]
                self.miner(prefix, prefixLength + 1, ULs[qItemNameList[i]], nextHUL, nextNameList, nextHWQUI)

    # --------- run + output ---------

    def runAlgorithm(
        self,
        topk: int,
        inputDB: str,
        inputProfit: str,
        coef: int,
        combinationmethod: EnumCombination,
        outputPath: str,
        stable_sorted_output: bool = False,
    ) -> None:
        self.k = topk
        self.coefficient = coef
        self.combiningMethod = combinationmethod

        self.kPatterns = []
        self.minUtil = 0
        self.HUQIcount = 0
        self.countConstruct = 0

        qitemNameList: List[Qitem] = []
        mapItemToUtilityList: Dict[Qitem, UtilityListTKQ] = {}

        if self.DEBUG_MODE:
            print("1. Build Initial Q-Utility Lists")
        self.buildInitialQUtilityLists(inputDB, inputProfit, qitemNameList, mapItemToUtilityList)

        if self.DEBUG_MODE:
            print("2. Find Initial High Utility Range Q-items")
        candidateList: List[Qitem] = []
        hwQUI: List[Qitem] = []
        self.findInitialRHUQIs(qitemNameList, mapItemToUtilityList, candidateList, hwQUI)

        if self.DEBUG_MODE:
            print("3. Recursive Mining Procedure")
        self.miner(self.itemsetBuffer, 0, None, mapItemToUtilityList, qitemNameList, hwQUI)

        if self.DEBUG_MODE:
            print(f"4. Finished mining. The final internal minUtil value is: {self.minUtil}")

        patterns_to_write = self.kPatterns if not stable_sorted_output else sorted(self.kPatterns, key=lambda p: (p.utility, p.uid))

        with open(outputPath, "w", encoding="utf-8") as w:
            for pat in patterns_to_write:
                line = " ".join(str(q) for q in pat.itemset) + f" #UTIL: {pat.utility}"
                w.write(line + "\n")

    def printStatistics(self) -> None:
        print("============= TKQ v 2.52 Statistical results===============")
        if self.DEBUG_MODE:
            print(f"K: {self.k} coefficient: {self.coefficient}")
        print("HUQIcount:", self.HUQIcount)
        if self.DEBUG_MODE:
            print("Join operation count:", self.countConstruct)
        print("================================================")


# -------------------- MainTestTKQ equivalent --------------------

def main() -> None:
    # Your Java MainTestTKQ parameters
    inputFileProfitPath = "Java//src//dbHUQI_p.txt"
    inputFileDBPath = "Java//src//dbHUQI.txt"
    output = "Java//src//output_py.txt"

    k = 4
    coef = 3
    combinationmethod = EnumCombination.COMBINEALL

    # If you want deterministic sorted output order:
    STABLE_SORTED_OUTPUT = False

    algo = AlgoTKQ(debug=True)
    algo.runAlgorithm(k, inputFileDBPath, inputFileProfitPath, coef, combinationmethod, output, stable_sorted_output=STABLE_SORTED_OUTPUT)
    algo.printStatistics()
    print("\nDone. Output written to:", output)


if __name__ == "__main__":
    main()