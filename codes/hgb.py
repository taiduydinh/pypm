#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Set, Iterable
import os
import sys
import time

# ---- Best-effort memory (mac/linux) ----
def _memory_mb_best_effort() -> float:
    try:
        import resource
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On macOS ru_maxrss is bytes; on Linux it is KB.
        # Detect macOS by presence of "darwin" in sys.platform.
        if sys.platform == "darwin":
            return r / (1024.0 * 1024.0)
        return r / 1024.0
    except Exception:
        return 0.0


# ----------------------------- MemoryLogger (simple) -----------------------------

class MemoryLogger:
    _instance = None

    def __init__(self):
        self._max_mem = 0.0

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self):
        self._max_mem = 0.0

    def checkMemory(self):
        m = _memory_mb_best_effort()
        if m > self._max_mem:
            self._max_mem = m

    def getMaxMemory(self):
        return self._max_mem


# ----------------------------- Core data structures -----------------------------

@dataclass(frozen=True)
class Element:
    tid: int
    iutils: int
    rutils: int


class UtilityList:
    def __init__(self, item: int):
        self.item = item
        self.sumIutils = 0
        self.sumRutils = 0
        self.exutil = 0
        self.elements: List[Element] = []

    def addElement(self, e: Element):
        self.sumIutils += e.iutils
        self.sumRutils += e.rutils
        self.elements.append(e)

    def setexutil(self, v: int):
        self.exutil = v


class Itemset:
    __slots__ = ("_items", "_utils", "acutility", "support")

    def __init__(self, items: Optional[Iterable[int]] = None, utils: Optional[Iterable[int]] = None,
                 acutility: int = 0, support: int = 0):
        items_list = list(items) if items is not None else []
        items_list.sort()
        self._items: Tuple[int, ...] = tuple(items_list)

        utils_list = list(utils) if utils is not None else []
        self._utils: Tuple[int, ...] = tuple(utils_list)

        self.acutility = acutility
        self.support = support

    def items(self) -> Tuple[int, ...]:
        return self._items

    def itemsUtilities(self) -> Tuple[int, ...]:
        return self._utils

    def size(self) -> int:
        return len(self._items)

    def contains(self, x: int) -> bool:
        return x in self._items

    def includedIn(self, other: "Itemset") -> bool:
        return set(self._items).issubset(other._items)

    def get(self, idx: int) -> int:
        return self._items[idx]

    def __hash__(self):
        return hash(self._items)

    def __eq__(self, other):
        return isinstance(other, Itemset) and self._items == other._items

    def clone(self) -> "Itemset":
        return Itemset(self._items, self._utils, self.acutility, self.support)

    def addItem(self, x: int) -> "Itemset":
        new_items = list(self._items) + [x]
        new_items.sort()
        return Itemset(new_items, self._utils, self.acutility, self.support)

    def union(self, other: "Itemset") -> "Itemset":
        return Itemset(sorted(set(self._items).union(other._items)))

    def cloneItemSetMinusAnItemset(self, to_remove: "Itemset") -> "Itemset":
        rem = set(to_remove._items)
        return Itemset([x for x in self._items if x not in rem])

    def __repr__(self):
        return f"Itemset(items={self._items}, util={self.acutility}, sup={self.support}, utils={self._utils})"


class HUTable:
    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.mapSupp: Dict[Itemset, int] = {}
        self.mapKey: Dict[Itemset, bool] = {}
        self.mapClosed: Dict[Itemset, bool] = {}

    def addHuighUtilityItemset(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        self.levels[itemset.size()].append(itemset)

    def getLevelFor(self, i: int) -> List[Itemset]:
        while len(self.levels) <= i:
            self.levels.append([])
        return self.levels[i]


class HUClosedTable:
    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.mapGenerators: Dict[Itemset, List[Itemset]] = {}

    def addHighUtilityClosedItemset(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        tmp = self.levels[itemset.size()]
        for e in tmp:
            if e.includedIn(itemset):
                return
        self.levels[itemset.size()].append(itemset)

    def getLevelFor(self, i: int) -> List[Itemset]:
        while len(self.levels) <= i + 1:
            self.levels.append([])
        return self.levels[i + 1]


# ----------------------------- HUCI-Miner -----------------------------

class AlgoFHIM_and_HUCI:
    def __init__(self):
        self.maxMemory = 0.0
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.hui = 0
        self.candidate = 0
        self.chui = 0
        self.ghui = 0
        self.maxlength = 0

        self.tableHUI = HUTable()
        self.tableHUCI = HUClosedTable()

        self.minUtility = 0
        self.algo = 1  # HUCI
        self.HG: List[Itemset] = []

        self.mapFMAP: Dict[int, Dict[int, int]] = {}
        self.mapItemToUtilityList: Dict[int, UtilityList] = {}
        self.mapLLFMAP: Dict[int, Dict[int, Dict[int, int]]] = {}

        self.dontOutputClosedItemsets = False
        self.dontOutputGeneratorItemsets = False

    def runAlgorithmHUCIMiner(self, input_path: str, output_path: Optional[str], minUti: int) -> HUClosedTable:
        return self._runAlgo(input_path, output_path, minUti, alg=1)

    def printStats(self):
        print("=============  HUCI-Miner ALGORITHM - STATS =============")
        print(f" Total time ~ {self.endTimestamp - self.startTimestamp} ms")
        print(f" Memory ~ {self.maxMemory} MB")
        print(f" Candidate count : {self.candidate}")
        print(f" High-utility itemsets count : {self.hui}")
        if not self.dontOutputClosedItemsets:
            print(f" Closed High-utility itemsets count : {self.chui}")
        print("===================================================")

    def _runAlgo(self, input_path: str, output_path: Optional[str], minUtility: int, alg: int) -> HUClosedTable:
        self.hui = 0
        self.candidate = 0
        self.chui = 0
        self.ghui = 0
        self.maxlength = 0

        self.algo = alg
        self.minUtility = minUtility

        self.startTimestamp = int(time.time() * 1000)
        print(f"Absolute utility threshold = {self.minUtility}")

        MemoryLogger.getInstance().reset()

        # 1st pass: TWU
        mapItemToTWU: Dict[int, int] = {}
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
                    mapItemToTWU[item] = mapItemToTWU.get(item, 0) + tu

        # 1-item utility lists
        listOfUtilityLists: List[UtilityList] = []
        self.mapItemToUtilityList = {}
        for item, twu in mapItemToTWU.items():
            if twu >= minUtility:
                ul = UtilityList(item)
                self.mapItemToUtilityList[item] = ul
                listOfUtilityLists.append(ul)
        listOfUtilityLists.sort(key=lambda ul: (mapItemToTWU[ul.item], ul.item))

        # 2nd pass: build UL + FMAP
        self.mapFMAP = {}
        tid = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                split = line.split(":")
                items = split[0].split()
                utils = split[2].split()
                remainingUtility = 0
                newTWU = 0
                revised = []
                for it_s, u_s in zip(items, utils):
                    it = int(it_s)
                    u = int(u_s)
                    if mapItemToTWU.get(it, 0) >= minUtility:
                        revised.append((it, u))
                        remainingUtility += u
                        newTWU += u

                revised.sort(key=lambda p: (mapItemToTWU[p[0]], p[0]))

                for i, (it, u) in enumerate(revised):
                    remainingUtility -= u
                    ul = self.mapItemToUtilityList[it]
                    ul.addElement(Element(tid, u, remainingUtility))

                    if it not in self.mapFMAP:
                        self.mapFMAP[it] = {}
                    fmap = self.mapFMAP[it]
                    for j in range(i + 1, len(revised)):
                        it_after, _ = revised[j]
                        fmap[it_after] = fmap.get(it_after, 0) + newTWU
                tid += 1

        MemoryLogger.getInstance().checkMemory()

        self.mapLLFMAP = {}
        prefix = Itemset()

        for i, X in enumerate(listOfUtilityLists):
            if X.sumIutils >= minUtility:
                self._store(prefix, X)

            if X.sumIutils + X.sumRutils >= minUtility:
                exULs: List[UtilityList] = []
                for j in range(i + 1, len(listOfUtilityLists)):
                    Y = listOfUtilityLists[j]
                    twuf = self.mapFMAP.get(X.item, {}).get(Y.item)
                    if twuf is not None and twuf < minUtility:
                        continue
                    self.candidate += 1
                    exULs.append(self._construct(None, X, Y))

                newPrefix = prefix.clone().addItem(X.item)
                if X.item not in self.mapLLFMAP:
                    self.mapLLFMAP[X.item] = {}
                self._huiMiner(X.item, True, newPrefix, X, exULs)
                self.mapLLFMAP[X.item] = None

        self._huciMiner()

        MemoryLogger.getInstance().checkMemory()
        self.maxMemory = MemoryLogger.getInstance().getMaxMemory()
        self.endTimestamp = int(time.time() * 1000)
        return self.tableHUCI

    def _findElementWithTID(self, ul: UtilityList, tid: int) -> Optional[Element]:
        lst = ul.elements
        lo, hi = 0, len(lst) - 1
        while lo <= hi:
            mid = (lo + hi) >> 1
            if lst[mid].tid < tid:
                lo = mid + 1
            elif lst[mid].tid > tid:
                hi = mid - 1
            else:
                return lst[mid]
        return None

    def _construct(self, P: Optional[UtilityList], px: UtilityList, py: UtilityList) -> UtilityList:
        pxy = UtilityList(py.item)
        newTWU = 0
        for ex in px.elements:
            ey = self._findElementWithTID(py, ex.tid)
            if ey is None:
                continue
            if P is None:
                pxy.addElement(Element(ex.tid, ex.iutils + ey.iutils, ey.rutils))
                newTWU += ex.iutils + ex.rutils
            else:
                e = self._findElementWithTID(P, ex.tid)
                if e is not None:
                    pxy.addElement(Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils))
                    newTWU += ex.iutils + ex.rutils
        pxy.setexutil(newTWU)
        return pxy

    def _store(self, prefix: Itemset, X: UtilityList):
        self.hui += 1
        newp = prefix.clone().addItem(X.item)
        self.maxlength = max(self.maxlength, newp.size())
        newp.acutility = X.sumIutils
        newp.support = len(X.elements)

        # utility unit array
        utils = []
        for it in newp.items():
            ul = self.mapItemToUtilityList[it]
            v = 0
            for e in X.elements:
                ey = self._findElementWithTID(ul, e.tid)
                if ey is not None:
                    v += ey.iutils
            utils.append(v)

        # rebuild (items already sorted)
        newp2 = Itemset(newp.items(), utils, newp.acutility, newp.support)
        self.tableHUI.addHuighUtilityItemset(newp2)
        self.tableHUI.mapKey[newp2] = True
        self.tableHUI.mapSupp[newp2] = newp2.support
        self.tableHUI.mapClosed[newp2] = True

    def _subset(self, s: List[Itemset], l: Itemset) -> List[Itemset]:
        res = []
        L = set(l.items())
        for it in s:
            if set(it.items()).issubset(L):
                res.append(it)
        return res

    def _huciMiner(self):
        for it_len in range(2, self.maxlength + 1):
            level = self.tableHUI.getLevelFor(it_len)
            prev = self.tableHUI.getLevelFor(it_len - 1)

            if level:
                for L in level:
                    for S in self._subset(prev, L):
                        if S.support == L.support:
                            self.tableHUI.mapClosed[S] = False
                            self.tableHUI.mapKey[L] = False

                for L in prev:
                    if self.tableHUI.mapClosed.get(L, False) is True:
                        self.tableHUCI.addHighUtilityClosedItemset(L)
                        self.chui += 1
                        gens = self._subset(self.HG, L)
                        self.tableHUCI.mapGenerators[L] = gens
                        # remove all gens
                        hgset = set(self.HG)
                        for g in gens:
                            hgset.discard(g)
                        self.HG = list(hgset)

                    if self.tableHUI.mapKey.get(L, False) is True and self.tableHUI.mapClosed.get(L, True) is False:
                        self.HG.append(L)
            else:
                for L in prev:
                    if self.tableHUI.mapClosed.get(L, False) is True:
                        self.tableHUCI.addHighUtilityClosedItemset(L)
                        self.chui += 1
                        gens = self._subset(self.HG, L)
                        self.tableHUCI.mapGenerators[L] = gens
                        hgset = set(self.HG)
                        for g in gens:
                            hgset.discard(g)
                        self.HG = list(hgset)

                    if self.tableHUI.mapKey.get(L, False) is True and self.tableHUI.mapClosed.get(L, True) is False:
                        self.HG.append(L)

        last = self.tableHUI.getLevelFor(self.maxlength)
        for L in last:
            if self.tableHUI.mapClosed.get(L, False) is True:
                self.tableHUCI.addHighUtilityClosedItemset(L)
                self.chui += 1
                gens = self._subset(self.HG, L)
                self.tableHUCI.mapGenerators[L] = gens
                hgset = set(self.HG)
                for g in gens:
                    hgset.discard(g)
                self.HG = list(hgset)

    def _huiMiner(self, k: int, ft: bool, prefix: Itemset, pUL: UtilityList, ULs: List[UtilityList]):
        for i in range(len(ULs) - 1, -1, -1):
            X = ULs[i]
            if X.sumIutils >= self.minUtility:
                self._store(prefix, X)
            if X.sumIutils + X.sumRutils >= self.minUtility:
                exULs = []
                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    if Y.exutil < self.minUtility:
                        continue
                    twuf = self.mapFMAP.get(X.item, {}).get(Y.item)
                    if twuf is not None and twuf < self.minUtility:
                        continue
                    self.candidate += 1
                    exULs.append(self._construct(pUL, X, Y))
                newPrefix = prefix.clone().addItem(X.item)
                self._huiMiner(k, True, newPrefix, X, exULs)


# ----------------------------- HGB rules -----------------------------

class Rule:
    def __init__(self, antecedent: Itemset, consequent: Itemset, utility: int,
                 uconf: float, parent: Itemset, antecedentUtility: int):
        self._ant = antecedent
        self._cons = consequent
        self._util = utility
        self._uconf = uconf
        self._parent = parent
        self._antutil = antecedentUtility

    def getAntecedent(self) -> Itemset:
        return self._ant

    def getConsequent(self) -> Itemset:
        return self._cons

    def getUtility(self) -> int:
        return self._util

    def getAntecedentUtility(self) -> int:
        return self._antutil

    def getConfidence(self) -> float:
        return self._uconf

    def getParent(self) -> Itemset:
        return self._parent


class Rules:
    def __init__(self, name: str):
        self.name = name
        self.rules: List[List[Rule]] = []
        self.count = 0

    def addRule(self, r: Rule):
        while len(self.rules) <= r.getParent().size():
            self.rules.append([])
        self.rules[r.getParent().size()].append(r)
        self.count += 1


class AlgoHGB:
    def __init__(self):
        self.closedPatternsAndGenerators: Optional[HUClosedTable] = None
        self.rules: Optional[Rules] = None
        self.minutility = 0
        self.uminconf = 0.0
        self.ruleCount = 0
        self.runtime = 0
        self.maxMemory = 0.0

    def printStats(self):
        print("=============  HGB ALGORITHM - STATS =============")
        print(f" Total time ~ {self.runtime} ms")
        print(f" Memory ~ {self.maxMemory} MB")
        print(f" High-utility association rule count : {self.ruleCount}")
        print("===================================================")

    def runAlgorithm(self, closedPatternsAndGenerators: HUClosedTable, minutility: int, uminconf: float) -> Rules:
        self.closedPatternsAndGenerators = closedPatternsAndGenerators
        self.minutility = minutility
        self.uminconf = uminconf
        self.rules = Rules("HGB Basis of association rules")
        self.ruleCount = 0

        MemoryLogger.getInstance().reset()
        start = time.time()

        for level in closedPatternsAndGenerators.levels:
            for itemset in level:
                if itemset.size() > 1:
                    self._processItemset(itemset)

        MemoryLogger.getInstance().checkMemory()
        self.maxMemory = MemoryLogger.getInstance().getMaxMemory()
        self.runtime = int((time.time() - start) * 1000)
        return self.rules

    def _add_minimal_premise(self, premises: Set[Itemset], cand: Itemset) -> None:
        cset = set(cand.items())
        for p in premises:
            pset = set(p.items())
            if pset.issubset(cset) and len(pset) < len(cset):
                return
        to_remove = []
        for p in premises:
            pset = set(p.items())
            if cset.issubset(pset) and len(cset) < len(pset):
                to_remove.append(p)
        for p in to_remove:
            premises.remove(p)
        premises.add(cand)

    def _shareUtility(self, itemsetToTest: Itemset) -> int:
        assert self.closedPatternsAndGenerators is not None
        smallclosed = []
        found = False
        testset = set(itemsetToTest.items())

        for level in self.closedPatternsAndGenerators.levels:
            if not level or level[0].size() < itemsetToTest.size():
                continue
            for it in level:
                if testset.issubset(it.items()):
                    smallclosed.append(it)
                    found = True
            if found:
                break

        if not smallclosed:
            return 0

        sc = min(smallclosed, key=lambda x: x.support)
        share = 0
        sc_items = sc.items()
        sc_utils = sc.itemsUtilities()
        for idx, it in enumerate(sc_items):
            if it in testset and idx < len(sc_utils):
                share += sc_utils[idx]
        return share

    def _findSupport(self, itemsetT: Itemset) -> int:
        assert self.closedPatternsAndGenerators is not None
        smallclosed = []
        found = False
        testset = set(itemsetT.items())

        for level in self.closedPatternsAndGenerators.levels:
            if not level or level[0].size() < itemsetT.size():
                continue
            for it in level:
                if testset.issubset(it.items()):
                    smallclosed.append(it)
                    found = True
            if found:
                break
        if not smallclosed:
            return 0
        return min(smallclosed, key=lambda x: x.support).support

    def _generateCandidateSizeK(self, levelK_1: Set[Itemset], g: Itemset) -> Set[Itemset]:
        cands = set()
        lvl = sorted(levelK_1, key=lambda s: s.items())
        if len(lvl) <= 1:
            return cands

        for i1 in lvl:
            for i2 in lvl:
                a = i1.items()
                b = i2.items()
                ok = True
                for k in range(len(a)):
                    if k == len(a) - 1:
                        if a[k] >= b[k]:
                            ok = False
                            break
                    else:
                        if a[k] < b[k]:
                            break
                        elif a[k] > b[k]:
                            ok = False
                            break
                if not ok:
                    continue

                missing = b[-1] if b else None
                if missing is None:
                    continue
                cand = Itemset(list(a[:-1]) + [missing])
                union = g.union(cand)
                sup = self._findSupport(union)
                if sup == g.support:
                    cands.add(cand)
        return cands

    def _apGenrules(self, k: int, m: int, lk: Itemset, Hm: Set[Itemset],
                    lSmallestPremise: Set[Itemset], g: Itemset, parentClosed: Itemset):
        if k > m + 1:
            Hm1 = self._generateCandidateSizeK(Hm, lk)
            Hm1_rec = set()
            for hm1 in Hm1:
                ant = g.union(hm1)
                uti = self._shareUtility(ant)

                share = 0
                ant_set = set(ant.items())
                parent_items = parentClosed.items()
                parent_utils = parentClosed.itemsUtilities()
                for idx, it in enumerate(parent_items):
                    if it in ant_set and idx < len(parent_utils):
                        share += parent_utils[idx]

                if uti >= self.minutility:
                    if uti != 0 and (share / uti) >= self.uminconf:
                        self._add_minimal_premise(lSmallestPremise, ant)
                else:
                    Hm1_rec.add(hm1)

            self._apGenrules(k, m + 1, lk, Hm1_rec, lSmallestPremise, g, parentClosed)

    def _processItemset(self, theItemset: Itemset):
        assert self.closedPatternsAndGenerators is not None
        assert self.rules is not None

        lSmallestPremise: Set[Itemset] = set()

        for j in range(0, theItemset.size() + 1):
            if j >= len(self.closedPatternsAndGenerators.levels):
                continue
            for i1 in self.closedPatternsAndGenerators.levels[j]:
                if set(i1.items()).issubset(theItemset.items()):
                    Rand = set()
                    find = False
                    gens = self.closedPatternsAndGenerators.mapGenerators.get(i1, [])

                    if len(gens) == 0:
                        share = 0
                        for idx, it in enumerate(theItemset.items()):
                            if i1.contains(it) and idx < len(theItemset.itemsUtilities()):
                                share += theItemset.itemsUtilities()[idx]
                        if i1.acutility != 0 and (share / i1.acutility) >= self.uminconf:
                            self._add_minimal_premise(lSmallestPremise, i1)
                    else:
                        for genI1 in gens:
                            share = 0
                            for idx, it in enumerate(theItemset.items()):
                                if genI1.contains(it) and idx < len(theItemset.itemsUtilities()):
                                    share += theItemset.itemsUtilities()[idx]
                            if genI1.acutility != 0 and (share / genI1.acutility) >= self.uminconf:
                                self._add_minimal_premise(lSmallestPremise, genI1)
                            else:
                                Rand.add(genI1)
                                find = True

                    if find:
                        for g in Rand:
                            lk = theItemset.cloneItemSetMinusAnItemset(g)
                            H1 = set(Itemset([it]) for it in lk.items())
                            k = len(H1)

                            H1_rec = set()
                            for hm1 in H1:
                                ant = g.union(hm1)
                                share = 0
                                ant_set = set(ant.items())
                                for idx, it in enumerate(theItemset.items()):
                                    if it in ant_set and idx < len(theItemset.itemsUtilities()):
                                        share += theItemset.itemsUtilities()[idx]
                                uti = self._shareUtility(ant)

                                if uti >= self.minutility:
                                    if uti != 0 and (share / uti) >= self.uminconf:
                                        self._add_minimal_premise(lSmallestPremise, ant)
                                else:
                                    H1_rec.add(hm1)

                            self._apGenrules(k, 1, lk, H1_rec, lSmallestPremise, g, theItemset)

        for gs in list(lSmallestPremise):
            cons_items = [it for it in theItemset.items() if it not in set(gs.items())]
            if not cons_items:
                continue
            consequent = Itemset(cons_items)

            share = 0
            gs_set = set(gs.items())
            for idx, it in enumerate(theItemset.items()):
                if it in gs_set and idx < len(theItemset.itemsUtilities()):
                    share += theItemset.itemsUtilities()[idx]

            uti = gs.acutility if gs.acutility != 0 else self._shareUtility(gs)
            if uti == 0:
                continue

            rule = Rule(gs, consequent, theItemset.acutility, share / uti, theItemset, uti)
            self.rules.addRule(rule)
            self.ruleCount += 1

    def writeRulesToFile(self, output_path: str):
        assert self.rules is not None
        with open(output_path, "w", encoding="utf-8") as w:
            out = []
            for ruleList in self.rules.rules:
                for r in ruleList:
                    ant = " ".join(map(str, r.getAntecedent().items()))
                    cons = " ".join(map(str, r.getConsequent().items()))
                    out.append(
                        f"{ant}\t==> {cons} #UTIL: {r.getUtility()} #AUTIL: {r.getAntecedentUtility()} #UCONF: {r.getConfidence()}"
                    )
            w.write("\n".join(out) + ("\n" if out else ""))


# ----------------------------- Main -----------------------------

def main():
    # Usage:
    # python3 hgb.py [input_path] [output_path] [min_utility] [minconf]
    input_path = sys.argv[1] if len(sys.argv) >= 2 else os.path.join(os.getcwd(), "Java//src//DB_Utility.txt")
    output_path = sys.argv[2] if len(sys.argv) >= 3 else os.path.join(os.getcwd(), "Java//src//output_py.txt")
    min_utility = int(sys.argv[3]) if len(sys.argv) >= 4 else 25
    minconf = float(sys.argv[4]) if len(sys.argv) >= 5 else 0.5

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Step 1: HUCI Miner
    huci = AlgoFHIM_and_HUCI()
    results = huci.runAlgorithmHUCIMiner(input_path, None, min_utility)
    huci.printStats()

    # Step 2: HGB
    algo = AlgoHGB()
    algo.runAlgorithm(results, min_utility, minconf)
    algo.writeRulesToFile(output_path)
    algo.printStats()


if __name__ == "__main__":
    main()