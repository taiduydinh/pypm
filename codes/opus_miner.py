# Opus_Miner.py
# Python implementation of the Opus-Miner algorithm for mining top-k non-redundant independently productive itemsets.

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Iterable, Optional, Tuple
import math
import heapq
import argparse
import time
import sys
import os

# ----------------------------- Memory Logger -----------------------------
class MemoryLogger:
    _instance: "MemoryLogger" | None = None

    def __init__(self) -> None:
        self._max = 0.0

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self) -> None:
        self._max = 0.0

    def checkMemory(self) -> float:
        try:
            import psutil  # optional
            rss = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        except Exception:
            rss = 0.0
        self._max = max(self._max, rss)
        return rss

    def getMaxMemory(self) -> float:
        return self._max


# ----------------------------- Tidset ------------------------------------
class Tidset(list):
    """Sorted list of integers representing TIDs."""

    def __init__(self, vals: Iterable[int] | None = None) -> None:
        super().__init__()
        if vals:
            super().extend(sorted(set(int(v) for v in vals)))

    @staticmethod
    def countIntersection(s1: "Tidset", s2: "Tidset") -> int:
        i = j = cnt = 0
        n1, n2 = len(s1), len(s2)
        while i < n1 and j < n2:
            a, b = s1[i], s2[j]
            if a == b:
                cnt += 1
                i += 1
                j += 1
            elif a < b:
                i += 1
            else:
                j += 1
        return cnt

    @staticmethod
    def intersection(result: "Tidset", s1: "Tidset", s2: "Tidset") -> None:
        result.clear()
        i = j = 0
        n1, n2 = len(s1), len(s2)
        while i < n1 and j < n2:
            a, b = s1[i], s2[j]
            if a == b:
                result.append(a)
                i += 1
                j += 1
            elif a < b:
                i += 1
            else:
                j += 1

    @staticmethod
    def dintersection(s1: "Tidset", s2: "Tidset") -> None:
        i = j = 0
        out: List[int] = []
        n1, n2 = len(s1), len(s2)
        while i < n1 and j < n2:
            a, b = s1[i], s2[j]
            if a == b:
                out.append(a)
                i += 1
                j += 1
            elif a < b:
                i += 1
            else:
                j += 1
        s1[:] = out

    @staticmethod
    def dunion(s1: "Tidset", s2: "Tidset") -> "Tidset":
        return Tidset(list(sorted(set(s1).union(set(s2)))))


# ----------------------------- Itemset -----------------------------------
class Itemset:
    """Sorted set of positive integers with stable ordering (like Java TreeSet)."""

    def __init__(self, it: Iterable[int] | None = None) -> None:
        self._data: List[int] = []
        if it:
            for x in it:
                self.add(int(x))
        self.count: int = 0
        self.value: float = 0.0
        self.p: float = 1.0
        self.self_sufficient: bool = True
        self._hash_cache: Optional[int] = None

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __contains__(self, x: int) -> bool:
        return self._bin_search(x) >= 0

    def add(self, x: int) -> None:
        idx = self._bin_search(x)
        if idx < 0:
            insert_at = ~idx
            self._data.insert(insert_at, x)
            self._hash_cache = None

    def remove(self, x: int) -> None:
        idx = self._bin_search(x)
        if idx >= 0:
            del self._data[idx]
            self._hash_cache = None

    def clear(self) -> None:
        self._data.clear()
        self._hash_cache = None

    def addAll(self, it: Iterable[int]) -> None:
        for x in it:
            self.add(int(x))

    def first(self) -> int:
        return self._data[0]

    def size(self) -> int:
        return len(self._data)

    def clone(self) -> "Itemset":
        c = Itemset(self._data)
        c.count = self.count
        c.value = self.value
        c.p = self.p
        c.self_sufficient = self.self_sufficient
        return c

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Itemset) and self._data == other._data

    def __hash__(self) -> int:
        if self._hash_cache is None:
            self._hash_cache = sum(self._data)
        return self._hash_cache

    def __repr__(self) -> str:
        return "{" + " ".join(map(str, self._data)) + "}"

    def _bin_search(self, x: int) -> int:
        lo, hi = 0, len(self._data) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            v = self._data[mid]
            if v == x:
                return mid
            if v < x:
                lo = mid + 1
            else:
                hi = mid - 1
        return ~lo


# ----------------------------- ItemsetRec ---------------------------------
@dataclass(order=True)
class ItemsetRec:
    value: float
    count: int = field(compare=False, default=0)
    p: float = field(compare=False, default=1.0)
    self_sufficient: bool = field(compare=False, default=True)
    items: Itemset = field(compare=False, default_factory=Itemset)

    def clone(self) -> "ItemsetRec":
        return ItemsetRec(
            value=self.value,
            count=self.count,
            p=self.p,
            self_sufficient=self.self_sufficient,
            items=self.items.clone(),
        )

    def size(self) -> int:
        return self.items.size()

    def add(self, x: int) -> None:
        self.items.add(x)

    def remove(self, x: int) -> None:
        self.items.remove(x)

    def clear(self) -> None:
        self.items.clear()

    def __iter__(self):
        return iter(self.items)

    def __contains__(self, x: int) -> bool:
        return x in self.items

    def __repr__(self) -> str:
        return f"{self.items} #SUP: {self.count} #VAL: {self.value} #P: {self.p}"


# ----------------------------- Global -------------------------------------
class Global:
    k: int = 100
    filter: bool = True
    correctionForMultCompare: bool = True
    noOfTransactions: int = 0
    noOfItems: int = 0
    tids: List[Tidset] = []
    alpha: List[float] = []
    itemNames: List[Optional[str]] = [None]
    searchByLift: bool = False
    redundancyTests: bool = True
    printClosures: bool = False

    @staticmethod
    def expandAlpha(depth: int) -> None:
        if not Global.alpha:
            Global.alpha = [1.0, 1.0]
            if depth <= 1:
                return
        if depth > Global.noOfItems:
            Global.alpha.append(0.0)
        elif depth == Global.noOfItems:
            Global.alpha.append(Global.alpha[depth - 1])
        else:
            for _i in range(len(Global.alpha), depth + 1):
                v = min((0.5 ** (depth - 1)) / math.exp(FisherTest.log_combin(Global.noOfItems, depth)) * 0.05,
                        Global.alpha[depth - 1])
                Global.alpha.append(v)

    @staticmethod
    def getAlpha(depth: int) -> float:
        if not Global.correctionForMultCompare:
            return 0.05
        if depth >= len(Global.alpha):
            Global.expandAlpha(depth)
        return Global.alpha[depth]


# ----------------------------- Fisher Test --------------------------------
class FisherTest:
    _lf: List[float] = [0.0]

    @staticmethod
    def logfact(n: int) -> float:
        lf = FisherTest._lf
        while len(lf) <= n:
            i = len(lf)
            lf.append(lf[-1] + math.log(i))
        return lf[n]

    @staticmethod
    def log_combin(n: int, k: int) -> float:
        return FisherTest.logfact(n) - FisherTest.logfact(k) - FisherTest.logfact(n - k)

    @staticmethod
    def fisherTest(a: int, b: int, c: int, d: int) -> float:
        if b < c:
            b, c = c, b
        invariant = -FisherTest.logfact(a + b + c + d) + FisherTest.logfact(a + b) + \
                    FisherTest.logfact(c + d) + FisherTest.logfact(a + c) + \
                    FisherTest.logfact(b + d)
        p = 0.0
        while c >= 0:
            p += math.exp(invariant - FisherTest.logfact(a) - FisherTest.logfact(b)
                          - FisherTest.logfact(c) - FisherTest.logfact(d))
            a += 1; b -= 1; c -= 1; d += 1
        return p


# ----------------------------- Utils --------------------------------------
class Utils:
    @staticmethod
    def subset(s1: ItemsetRec, s2: ItemsetRec) -> bool:
        it1 = iter(s1.items)
        it2 = iter(s2.items)
        try:
            val1 = next(it1)
        except StopIteration:
            return True
        try:
            val2 = next(it2)
        except StopIteration:
            return False
        i, j = s1.size(), s2.size()
        while i > 0:
            if val1 < val2:
                return False
            if val1 == val2:
                if i == 1:
                    break
                val1 = next(it1)
                i -= 1
            if j == 1:
                return False
            val2 = next(it2)
            j -= 1
        return True

    @staticmethod
    def gettids(iset: Itemset | ItemsetRec, _t: Tidset | None = None) -> Tidset:
        items = list(iset.items if isinstance(iset, ItemsetRec) else iset)
        assert len(items) > 0
        if len(items) == 1:
            return Global.tids[items[0]]
        res = Tidset()
        Tidset.intersection(res, Global.tids[items[0]], Global.tids[items[1]])
        for x in items[2:]:
            Tidset.dintersection(res, Global.tids[x])
        return res

    @staticmethod
    def countToSup(count: int) -> float:
        return count / float(Global.noOfTransactions) if Global.noOfTransactions else 0.0

    @staticmethod
    def itemSup(item: int) -> float:
        return Utils.countToSup(len(Global.tids[item]))

    @staticmethod
    def fisher(count: int, count1: int, count2: int) -> float:
        return FisherTest.fisherTest(Global.noOfTransactions - count1 - count2 + count,
                                     count1 - count, count2 - count, count)


# ----------------------------- Queue --------------------------------------
@dataclass
class ItemQElement:
    ubVal: float
    item: int


class ItemQClass(list):
    def insert_by_ub(self, ubVal: float, item: int) -> None:
        if not self:
            self.append(ItemQElement(ubVal, item)); return
        lo, hi = 0, len(self) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if ubVal <= self[mid].ubVal:
                lo = mid + 1
            else:
                hi = mid
        if self[lo].ubVal >= ubVal:
            lo += 1
        self.append(ItemQElement(-9999.0, -1))
        for j in range(len(self) - 1, lo, -1):
            self[j] = self[j - 1]
        self[lo] = ItemQElement(ubVal, item)

    def append_item(self, ubVal: float, item: int) -> None:
        self.append(ItemQElement(ubVal, item))

    def sort_desc(self) -> None:
        self.sort(key=lambda e: e.ubVal, reverse=True)


# ----------------------------- Load Data ----------------------------------
class LoadData:
    @staticmethod
    def _reset():
        Global.noOfTransactions = 0
        Global.tids = [Tidset()]  # index 0 unused
        Global.noOfItems = 0

    @staticmethod
    def load_data(filename: str) -> None:
        LoadData._reset()
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                Global.noOfTransactions += 1
                tid = Global.noOfTransactions
                for tok in line.split():
                    item = int(tok)
                    while len(Global.tids) < item + 1:
                        Global.tids.append(Tidset())
                    Global.tids[item].append(tid)
        Global.noOfItems = len(Global.tids) - 1

    @staticmethod
    def loadCSVdata(filename: str) -> None:
        LoadData._reset()
        Global.itemNames = [None]
        name_to_item: Dict[str, int] = {}
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                Global.noOfTransactions += 1
                tid = Global.noOfTransactions
                for name in line.split():
                    if name not in name_to_item:
                        name_to_item[name] = len(name_to_item) + 1
                        Global.itemNames.append(name)
                    item = name_to_item[name]
                    while len(Global.tids) < item + 1:
                        Global.tids.append(Tidset())
                    Global.tids[item].append(tid)
        Global.noOfItems = len(Global.tids) - 1


# ----------------------------- Find Closure -------------------------------
class FindClosure:
    @staticmethod
    def find_closure(iset: Itemset, closure: Itemset) -> None:
        closure.addAll(iset)
        thistids = Utils.gettids(iset, None)
        for item in range(1, Global.noOfItems + 1):
            if len(Global.tids[item]) >= len(thistids) and item not in iset:
                if Tidset.countIntersection(thistids, Global.tids[item]) == len(thistids):
                    closure.add(item)


# ----------------------------- Filter Itemsets ----------------------------
class FilterItemsets:
    @staticmethod
    def _set_difference(set1: Tidset, set2: Tidset) -> Tidset:
        res = Tidset()
        i = j = 0
        while i < len(set1) and j < len(set2):
            a, b = set1[i], set2[j]
            if a == b:
                i += 1; j += 1
            elif a < b:
                res.append(a); i += 1
            else:
                j += 1
        while i < len(set1):
            res.append(set1[i]); i += 1
        return res

    @staticmethod
    def checkSS2(uniqueTids: List[Tidset], no: int,
                 tidsleft: Tidset, tidsright: Tidset,
                 availabletids: int, count: int, alpha: float) -> bool:
        if no == 0:
            p = FisherTest.fisherTest(availabletids - len(tidsleft) - len(tidsright) + count,
                                      len(tidsleft) - count, len(tidsright) - count, count)
            return p <= alpha
        newtids = Tidset()
        Tidset.intersection(newtids, uniqueTids[no - 1], tidsleft)
        if not FilterItemsets.checkSS2(uniqueTids, no - 1, newtids, tidsright, availabletids, count, alpha):
            return False
        newtids = Tidset()
        Tidset.intersection(newtids, uniqueTids[no - 1], tidsright)
        if not FilterItemsets.checkSS2(uniqueTids, no - 1, tidsleft, newtids, availabletids, count, alpha):
            return False
        return True

    @staticmethod
    def checkSS(iset: ItemsetRec, supsettids: Tidset) -> bool:
        items = list(iset.items)
        uniqueTids: List[Tidset] = [Tidset() for _ in items]
        for i, it in enumerate(items):
            uniqueTids[i] = FilterItemsets._set_difference(Global.tids[it], supsettids)
            if len(uniqueTids[i]) == 0:
                return False
        uniqueCov = Tidset(uniqueTids[0])
        for i in range(1, len(items)):
            Tidset.dintersection(uniqueCov, uniqueTids[i])
        tidsright = uniqueTids[-1]
        for i in range(len(uniqueTids) - 2, -1, -1):
            ok = FilterItemsets.checkSS2(uniqueTids, i,
                                         uniqueTids[i], tidsright,
                                         Global.noOfTransactions - len(supsettids),
                                         len(uniqueCov),
                                         Global.getAlpha(len(iset.items)))
            if not ok:
                return False
            if i > 0:
                Tidset.dintersection(tidsright, uniqueTids[i])
        return True

    @staticmethod
    def filter_itemsets(all_is: List[ItemsetRec]) -> None:
        if not all_is:
            return
        all_is.sort(key=lambda r: r.size(), reverse=True)
        for i in range(1, len(all_is)):
            current = all_is[i]
            supsettids = Tidset()
            for j in range(0, i):
                sup = all_is[j]
                if sup.self_sufficient and Utils.subset(current, sup):
                    supitems = [x for x in sup if x not in current]
                    if supitems:
                        thissup = Utils.gettids(Itemset(supitems), None)
                        x = Tidset(thissup)
                        supsettids = x if not supsettids else Tidset.dunion(supsettids, x)
            if supsettids and not FilterItemsets.checkSS(current, supsettids):
                current.self_sufficient = False


# ----------------------------- Find Itemsets ------------------------------
class FindItemsets:
    minValue: float = -float("inf")
    TIDCount: Dict[Tuple[int, ...], int] = {}

    @staticmethod
    def _key(iset: Itemset) -> Tuple[int, ...]:
        return tuple(iset)

    @staticmethod
    def getTIDCount(iset: Itemset) -> Optional[int]:
        if iset.size() == 1:
            return len(Global.tids[iset.first()])
        return FindItemsets.TIDCount.get(FindItemsets._key(iset))

    @dataclass
    class RedundantAprioriFlags:
        redundant: bool = False
        apriori: bool = False

    @dataclass
    class ValP:
        val: float = -float("inf")
        p: float = 0.0

    @dataclass
    class SoFarRemaining:
        sofar: Itemset = field(default_factory=Itemset)
        remaining: Itemset = field(default_factory=Itemset)

    @staticmethod
    def checkImmediateSubsets(iset: ItemsetRec, isCnt: int, flags: "FindItemsets.RedundantAprioriFlags") -> None:
        subset = iset.items.clone()
        flags.redundant = False
        flags.apriori = False
        for x in list(iset.items):
            subset.remove(x)
            subsetCnt = FindItemsets.getTIDCount(subset)
            if subsetCnt is None:
                flags.redundant = True
                flags.apriori = True
                return
            if Global.redundancyTests and subsetCnt == isCnt:
                flags.redundant = True
            subset.add(x)

    @staticmethod
    def checkSubsetsX(sfr: "FindItemsets.SoFarRemaining", limit: int, cnt: int, new_sup: float,
                      valp: "FindItemsets.ValP", alpha: float) -> bool:
        sofarCnt = FindItemsets.getTIDCount(sfr.sofar)
        remainingCnt = FindItemsets.getTIDCount(sfr.remaining)
        if sofarCnt is None or remainingCnt is None:
            return False
        this_val = (new_sup / (Utils.countToSup(remainingCnt) * Utils.countToSup(sofarCnt))
                    if Global.searchByLift else
                    new_sup - Utils.countToSup(remainingCnt) * Utils.countToSup(sofarCnt))
        if this_val < valp.val:
            valp.val = this_val
            if this_val <= FindItemsets.minValue:
                return False
        this_p = Utils.fisher(cnt, sofarCnt, remainingCnt)
        if this_p > valp.p:
            valp.p = this_p
            if valp.p > alpha:
                return False
        if sfr.remaining.size() > 1:
            new_remaining = sfr.remaining.clone()
            for current in list(sfr.remaining):
                if current >= limit:
                    break
                sfr.sofar.add(current)
                new_remaining.remove(current)
                new_sfr = FindItemsets.SoFarRemaining(sfr.sofar, new_remaining)
                if not FindItemsets.checkSubsetsX(new_sfr, current, cnt, new_sup, valp, alpha):
                    return False
                sfr.sofar.remove(current)
                new_remaining.add(current)
        return valp.p <= alpha and valp.val > FindItemsets.minValue

    @staticmethod
    def checkSubsets(item: int, iset: ItemsetRec, cnt: int, new_sup: float,
                     parentCnt: int, parentSup: float, valp: "FindItemsets.ValP", alpha: float) -> bool:
        assert iset.size() > 1
        itemCnt = len(Global.tids[item])
        valp.val = (new_sup / (parentSup * Utils.itemSup(item))
                    if Global.searchByLift else
                    new_sup - parentSup * Utils.itemSup(item))
        if valp.val <= FindItemsets.minValue:
            return False
        valp.p = Utils.fisher(cnt, itemCnt, parentCnt)
        if valp.p > alpha:
            return False
        if iset.size() > 2:
            remaining = Itemset(iset.items)
            remaining.remove(item)
            sofar = Itemset([item])
            for x in list(iset.items):
                if x == item:
                    continue
                sofar.add(x)
                remaining.remove(x)
                if not FindItemsets.checkSubsetsX(FindItemsets.SoFarRemaining(sofar, remaining),
                                                  x, cnt, new_sup, valp, alpha):
                    return False
                sofar.remove(x)
                remaining.add(x)
        return valp.p <= alpha and valp.val > FindItemsets.minValue

    heap: List[ItemsetRec] = []

    @staticmethod
    def insert_itemset(rec: ItemsetRec) -> None:
        if len(FindItemsets.heap) >= Global.k:
            heapq.heappop(FindItemsets.heap)
        heapq.heappush(FindItemsets.heap, rec.clone())
        if len(FindItemsets.heap) == Global.k:
            newMin = FindItemsets.heap[0].value
            if newMin > FindItemsets.minValue:
                FindItemsets.minValue = newMin

    @dataclass
    class CoverIsQ:
        iset: ItemsetRec
        cover: Tidset
        q: ItemQClass

    @staticmethod
    def opus(cq: "FindItemsets.CoverIsQ", maxItemCount: int) -> None:
        parentSup = Utils.countToSup(len(cq.cover))
        depth = cq.iset.size() + 1
        newCover = Tidset()
        newQ = ItemQClass()
        for i in range(len(cq.q)):
            item = cq.q[i].item
            Tidset.intersection(newCover, cq.cover, Global.tids[item])
            count = len(newCover)
            newMaxItemCount = max(maxItemCount, len(Global.tids[item]))
            new_sup = Utils.countToSup(count)
            lb_p = Utils.fisher(count, newMaxItemCount, count)
            ubval = (0.0 if count == 0 else 1.0 / Utils.countToSup(maxItemCount)) if Global.searchByLift \
                else new_sup - new_sup * Utils.countToSup(maxItemCount)
            if lb_p <= Global.getAlpha(depth) and ubval > FindItemsets.minValue:
                cq.iset.add(item)
                flags = FindItemsets.RedundantAprioriFlags()
                FindItemsets.checkImmediateSubsets(cq.iset, count, flags)
                if not flags.apriori:
                    valp = FindItemsets.ValP(-float("inf"), 0.0)
                    if FindItemsets.checkSubsets(item, cq.iset, count, new_sup, len(cq.cover),
                                                 parentSup, valp, Global.getAlpha(depth)):
                        cq.iset.count = count
                        cq.iset.value = valp.val
                        cq.iset.p = valp.p
                        FindItemsets.insert_itemset(cq.iset)
                    if not flags.redundant:
                        FindItemsets.TIDCount[FindItemsets._key(cq.iset.items)] = count
                        if newQ:
                            FindItemsets.opus(FindItemsets.CoverIsQ(cq.iset.clone(), Tidset(newCover), ItemQClass(newQ)),
                                              newMaxItemCount)
                        newQ.insert_by_ub(ubval, item)
                cq.iset.remove(item)
        MemoryLogger.getInstance().checkMemory()

    @staticmethod
    def find_itemsets() -> None:
        q = ItemQClass()
        for i in range(1, Global.noOfItems + 1):
            c = len(Global.tids[i])
            sup = Utils.countToSup(c)
            ubVal = (1.0 / sup) if Global.searchByLift else (sup - sup * sup)
            if Utils.fisher(c, c, c) <= Global.getAlpha(2):
                q.append_item(ubVal, i)
        newq = ItemQClass()
        if q:
            q.sort_desc()
            newq.insert_by_ub(q[0].ubVal, q[0].item)
        prevMinVal = FindItemsets.minValue
        isrec = ItemsetRec(0.0, 0, 1.0, True, Itemset())
        for i in range(1, len(q)):
            if q[i].ubVal <= FindItemsets.minValue:
                break
            item = q[i].item
            isrec.clear()
            isrec.add(item)
            cover = Global.tids[item]
            FindItemsets.opus(FindItemsets.CoverIsQ(isrec.clone(), Tidset(cover), ItemQClass(newq)), len(cover))
            newq.append_item(q[i].ubVal, item)
            if prevMinVal < FindItemsets.minValue:
                print(f"<{FindItemsets.minValue}>", end="")
                prevMinVal = FindItemsets.minValue
            else:
                print(".", end="")
        print()


# ----------------------------- Printing -----------------------------------
class PrintItemsets:
    @staticmethod
    def _print_itemset(f, iset: Itemset | ItemsetRec, csv_names: bool) -> None:
        items = list(iset.items if isinstance(iset, ItemsetRec) else iset)
        out = []
        for x in items:
            out.append(Global.itemNames[x] if csv_names and x < len(Global.itemNames) else str(x))
        f.write(" ".join(out))

    @staticmethod
    def _print_rec(f, rec: ItemsetRec, csv_names: bool, searchByLift: bool) -> None:
        PrintItemsets._print_itemset(f, rec, csv_names)
        measure = " #LIFT: " if searchByLift else " #LEVERAGE: "
        f.write(f" #SUP: {rec.count}{measure}{rec.value} #PVALUE: {rec.p}")
        if Global.printClosures:
            closure = Itemset()
            FindClosure.find_closure(rec.items, closure)
            if closure.size() > rec.size():
                f.write(" #CLOSURE: ")
                PrintItemsets._print_itemset(f, closure, csv_names)
        f.write("\n")

    @staticmethod
    def print_all(path: str, recs: List[ItemsetRec], csv_names: bool, searchByLift: bool) -> None:
        recs_sorted = sorted(recs, key=lambda r: r.value, reverse=True)
        with open(path, "w", encoding="utf-8") as f:
            failed = 0
            for r in recs_sorted:
                if r.self_sufficient:
                    PrintItemsets._print_rec(f, r, csv_names, searchByLift)
                else:
                    failed += 1
            if failed:
                f.write(f"\n{failed} itemsets failed test for self sufficiency\n")
                for r in recs_sorted:
                    if not r.self_sufficient:
                        PrintItemsets._print_rec(f, r, csv_names, searchByLift)


# ----------------------------- Algo Opus Miner ----------------------------
class AlgoOpusMiner:
    itemsets_heap: List[ItemsetRec] = []

    def __init__(self) -> None:
        self.start_ts = 0.0
        self.end_ts = 0.0
        self.nonRedundantProductiveItemsetsCount = 0
        self.DEBUG = False

    def runAlgorithm(self, inputFileName: str, outputFileName: str,
                     printClosure: bool, filter_out: bool, k: int,
                     searchByLift: bool, correctionForMultiCompare: bool,
                     redundancyTests: bool, isCSVInputFile: bool) -> None:
        Global.correctionForMultCompare = correctionForMultiCompare
        Global.printClosures = printClosure
        Global.filter = filter_out
        Global.k = k
        Global.searchByLift = searchByLift
        Global.redundancyTests = redundancyTests

        AlgoOpusMiner.itemsets_heap.clear()
        FindItemsets.heap = AlgoOpusMiner.itemsets_heap
        FindItemsets.minValue = -float("inf")
        FindItemsets.TIDCount.clear()

        print(f"Loading data from {inputFileName}")
        self.start_ts = time.time() * 1000
        MemoryLogger.getInstance().checkMemory()

        if isCSVInputFile:
            LoadData.loadCSVdata(inputFileName)
        else:
            LoadData.load_data(inputFileName)

        print(f"{Global.noOfTransactions} transactions, {Global.noOfItems} items")

        print("Finding itemsets")
        FindItemsets.find_itemsets()

        results: List[ItemsetRec] = []
        while AlgoOpusMiner.itemsets_heap:
            results.append(heapq.heappop(AlgoOpusMiner.itemsets_heap))
        results.sort(key=lambda r: r.value, reverse=True)

        if filter_out:
            print("Filtering itemsets")
            FilterItemsets.filter_itemsets(results)

        self.nonRedundantProductiveItemsetsCount = len(results)

        print("Printing itemsets")
        PrintItemsets.print_all(outputFileName, results, isCSVInputFile, searchByLift)
        MemoryLogger.getInstance().checkMemory()
        self.end_ts = time.time() * 1000

    def printStats(self) -> None:
        nonRedundant = "Non-redundant" if Global.redundancyTests else ""
        independently = " Independently" if Global.filter else ""
        print("=============  Opus-Miner algorithm (Python) - STATS =======")
        print(f" {nonRedundant}{independently} productive itemset count: {self.nonRedundantProductiveItemsetsCount}")
        for i in range(2, len(Global.alpha)):
            print(f"  Alpha for size {i} {Global.alpha[i]}")
        print(f" Total time ~ {int(self.end_ts - self.start_ts)} ms")
        print(f" Max Memory ~ {MemoryLogger.getInstance().getMaxMemory():.2f} MB")
        print(f" Transaction count: {Global.noOfTransactions} Item count: {Global.noOfItems}")
        print("=============================================================")


# ----------------------------- CLI (now optional) -------------------------
def _auto_defaults(script_dir: str) -> tuple[str, bool, str]:
    """
    Returns (input_path, csv_flag, output_path).
    Priority:
      1) contextOpusMiner.txt (numeric)
      2) demo.csv (names, csv=True)
    """
    candidate_txt = os.path.join(script_dir, "contextOpusMiner.txt")
    candidate_csv = os.path.join(script_dir, "demo.csv")
    if os.path.isfile(candidate_txt):
        return candidate_txt, False, os.path.join(script_dir, "output_python.txt")
    if os.path.isfile(candidate_csv):
        return candidate_csv, True, os.path.join(script_dir, "output_python.txt")
    # fallback: try current working directory
    cwd_txt = os.path.join(os.getcwd(), "contextOpusMiner.txt")
    cwd_csv = os.path.join(os.getcwd(), "demo.csv")
    if os.path.isfile(cwd_txt):
        return cwd_txt, False, os.path.join(os.getcwd(), "output_python.txt")
    if os.path.isfile(cwd_csv):
        return cwd_csv, True, os.path.join(os.getcwd(), "output_python.txt")
    # last resort: raise with clear message
    raise FileNotFoundError(
        "No input provided and could not find 'contextOpusMiner.txt' or 'demo.csv' next to the script or in the CWD."
    )


def main():
    parser = argparse.ArgumentParser(description="Opus-Miner (Python)", add_help=True)
    parser.add_argument("--input", help="Input file (space-separated items per transaction)")
    parser.add_argument("--output", help="Output file to write itemsets")
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument("--filter", action="store_true", help="Filter itemsets for self-sufficiency")
    parser.add_argument("--no-filter", dest="filter", action="store_false")
    parser.add_argument("--lift", action="store_true", help="Search by lift (default is leverage)")
    parser.add_argument("--leverage", dest="lift", action="store_false")
    parser.add_argument("--redundancy", action="store_true", help="Enable redundancy tests")
    parser.add_argument("--no-redundancy", dest="redundancy", action="store_false")
    parser.add_argument("--alpha-correction", action="store_true")
    parser.add_argument("--no-alpha-correction", dest="alpha_correction", action="store_false")
    parser.add_argument("--csv", action="store_true", help="Treat items as names (space-separated)")
    parser.set_defaults(filter=True, lift=False, redundancy=True, alpha_correction=True)

    args = parser.parse_args()

    # If input or output not provided, pick intelligent defaults.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if not args.input or not args.output:
        try:
            auto_in, auto_csv, auto_out = _auto_defaults(script_dir)
            if not args.input:
                args.input = auto_in
                if auto_csv:
                    args.csv = True
            if not args.output:
                args.output = auto_out
            print(f"[auto] Using input='{args.input}', output='{args.output}', csv={args.csv}")
        except FileNotFoundError as e:
            print(str(e))
            sys.exit(2)

    algo = AlgoOpusMiner()
    algo.runAlgorithm(
        inputFileName=args.input,
        outputFileName=args.output,
        printClosure=False,
        filter_out=args.filter,
        k=args.k,
        searchByLift=args.lift,
        correctionForMultiCompare=args.alpha_correction,
        redundancyTests=args.redundancy,
        isCSVInputFile=args.csv,
    )
    algo.printStats()


if __name__ == "__main__":
    main()
