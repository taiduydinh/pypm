#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, Iterable
import argparse
import time
import math
import os


# =========================================================
# MemoryLogger (SPMF style)
# =========================================================
class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

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
        # Lightweight fallback (no psutil). We keep this as 0.0 and still track "max".
        current = 0.0
        if current > self.maxMemoryMB:
            self.maxMemoryMB = current
        return current

    def getMaxMemory(self) -> float:
        return self.maxMemoryMB


# =========================================================
# Data structures
# =========================================================
@dataclass
class Pair:
    estimatedUtility: List[Optional[int]]
    exactUtility: int = 0

    @staticmethod
    def with_periods(period_count: int) -> "Pair":
        return Pair(estimatedUtility=[None] * period_count, exactUtility=0)


@dataclass
class ItemUtility:
    item: int
    utility: int

    def __str__(self) -> str:
        return f"[{self.item},{self.utility}]"


class TransactionWithPeriod:
    def __init__(self, items_utilities: List[ItemUtility], transaction_utility: int, period: int) -> None:
        self.itemsUtilities: List[ItemUtility] = items_utilities
        self.transactionUtility: int = transaction_utility  # positive-only TU (per your Java bug-fix)
        self._period: int = period

    def getPeriod(self) -> int:
        return self._period

    def getItems(self) -> List[ItemUtility]:
        return self.itemsUtilities

    def get(self, index: int) -> ItemUtility:
        return self.itemsUtilities[index]

    def size(self) -> int:
        return len(self.itemsUtilities)

    def getItemsUtilities(self) -> List[ItemUtility]:
        return self.itemsUtilities

    def getTransactionUtility(self) -> int:
        return self.transactionUtility

    def contains(self, item: int) -> bool:
        # assumes lexical order; early break when larger encountered
        for iu in self.itemsUtilities:
            if iu.item == item:
                return True
            if iu.item > item:
                return False
        return False

    def __str__(self) -> str:
        # Keep close to Java toString()
        left = " ".join(str(x) for x in self.itemsUtilities)
        right = " ".join(str(x) for x in self.itemsUtilities)
        return f"{left} :{self.transactionUtility}: {right}"


@dataclass
class PeriodUtility:
    period: int
    utility: int


class ItemsetTP:
    def __init__(self, items: Iterable[int]) -> None:
        self.items: Tuple[int, ...] = tuple(items)
        self.utility: int = 0
        self.listPeriodUtility: Optional[List[PeriodUtility]] = None

    def setPeriodUtility(self, period: int, exactUtility: int) -> None:
        if self.listPeriodUtility is None:
            self.listPeriodUtility = []
        self.listPeriodUtility.append(PeriodUtility(period=period, utility=exactUtility))

    def getItems(self) -> Tuple[int, ...]:
        return self.items

    def size(self) -> int:
        return len(self.items)

    def getUtility(self) -> int:
        return self.utility

    def incrementUtility(self, increment: int) -> None:
        self.utility += increment

    def __str__(self) -> str:
        return " ".join(str(x) for x in self.items) + " "


class HashTable:
    def __init__(self, size: int) -> None:
        self.table: List[Optional[List[ItemsetTP]]] = [None] * size

    @staticmethod
    def same(itemset1: Tuple[int, ...] | List[int], itemset2: List[int] | Tuple[int, ...]) -> bool:
        if len(itemset1) != len(itemset2):
            return False
        # Java loops to length-1 (buggy) but intent is full equality for ordered itemsets
        for i in range(len(itemset1)):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    def retrieveItemset(self, itemset: List[int], hashcode: int) -> Optional[ItemsetTP]:
        bucket = self.table[hashcode]
        if bucket is None:
            return None
        for existing in bucket:
            if HashTable.same(existing.getItems(), itemset):
                return existing
        return None

    def put(self, itemset: ItemsetTP, hashcode: int) -> None:
        if self.table[hashcode] is None:
            self.table[hashcode] = []
        self.table[hashcode].append(itemset)

    def hashCode(self, itemset: List[int]) -> int:
        hashcode = 0
        for v in itemset:
            hashcode += v * 3
        if hashcode < 0:
            hashcode = -hashcode
        return hashcode % len(self.table)


# =========================================================
# Database with periods
# =========================================================
class DatabaseWithPeriods:
    def __init__(self, period_count: int) -> None:
        self.periodCount: int = period_count

        self._allItems: Set[int] = set()
        self._allNegativeItems: Set[int] = set()
        self._transactions: List[TransactionWithPeriod] = []

        self._periodsTotalUtilities: List[int] = [0] * period_count
        self._mapItemPeriods: Dict[int, int] = {}  # item -> bitmask of periods (BitSet replacement)
        self._mapItemUtility: Dict[int, Pair] = {}  # item -> Pair(exact/estimated)

        self.smallestID: int = 2**31 - 1
        self.largestID: int = 0

        # scalability experiment
        self.maxSEQUENCECOUNT: int = 2**31 - 1

    # --- getters (match Java naming) ---
    def getMapItemExactEstUtility(self) -> Dict[int, Pair]:
        return self._mapItemUtility

    def getPeriodUtilities(self) -> List[int]:
        return self._periodsTotalUtilities

    def getMapItemPeriod(self) -> Dict[int, int]:
        return self._mapItemPeriods

    def getNegativeItems(self) -> Set[int]:
        return self._allNegativeItems

    def getPeriodCount(self) -> int:
        return self.periodCount

    def getPeriodUtility(self, period: int) -> int:
        return self._periodsTotalUtilities[period]

    def getTransactions(self) -> List[TransactionWithPeriod]:
        return self._transactions

    def getAllItems(self) -> Set[int]:
        return self._allItems

    def size(self) -> int:
        return len(self._transactions)

    # --- internal helpers ---
    def _addPeriodToItem(self, period: int, item: int) -> None:
        mask = self._mapItemPeriods.get(item, 0)
        mask |= (1 << period)
        self._mapItemPeriods[item] = mask

    def _incrementPeriodUtility(self, period: int, transactionUtility: int) -> None:
        # Java supports discovering periods dynamically; here periodCount is fixed.
        self._periodsTotalUtilities[period] += transactionUtility

    def loadFile(self, path: str) -> None:
        tid = 0
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                if tid >= self.maxSEQUENCECOUNT:
                    break
                line = raw.strip()
                if not line or line[0] in "#%@":
                    continue
                parts = line.split(":")
                # Expect at least 4 parts: items, TU, utils, period
                if len(parts) < 4:
                    raise ValueError(f"Bad line (need items:TU:utils:period): {line}")
                self._processTransaction(parts)
                tid += 1

    def _processTransaction(self, parts: List[str]) -> None:
        items_str = parts[0].strip().split()
        utils_str = parts[2].strip().split()
        period = int(parts[3].strip())

        if period >= self.periodCount:
            raise ValueError(
                'ERROR: the parameter "period count" should not be smaller than the number of periods '
                "in the input file. Please run again with correct period count."
            )

        if len(items_str) != len(utils_str):
            raise ValueError(f"Items/utilities length mismatch: {parts[0]} :: {parts[2]}")

        # Bug-fix like Java:
        # - TU with negative+positive for relative utility denominator (period totals)
        # - TU with positive-only for upper bounds (estimated utilities)
        utility_values = [int(x) for x in utils_str]
        tu_neg_pos = sum(utility_values)
        tu_pos_only = sum(u for u in utility_values if u > 0)

        item_utility_objects: List[ItemUtility] = []
        for item_s, util in zip(items_str, utility_values):
            item = int(item_s)
            item_utility_objects.append(ItemUtility(item=item, utility=util))

            if util < 0:
                self._allNegativeItems.add(item)

            self.smallestID = min(self.smallestID, item)
            self.largestID = max(self.largestID, item)
            self._allItems.add(item)

            pair = self._mapItemUtility.get(item)
            if pair is None:
                pair = Pair.with_periods(self.periodCount)
                self._mapItemUtility[item] = pair

            pair.exactUtility += util
            if pair.estimatedUtility[period] is None:
                pair.estimatedUtility[period] = tu_pos_only
            else:
                pair.estimatedUtility[period] += tu_pos_only

            self._addPeriodToItem(period, item)

        # Store transaction utility as positive-only (per Java)
        self._transactions.append(TransactionWithPeriod(item_utility_objects, tu_pos_only, period))
        self._incrementPeriodUtility(period, tu_neg_pos)


# =========================================================
# AlgoTSHoun
# =========================================================
class AlgoTSHoun:
    def __init__(self) -> None:
        self.database: Optional[DatabaseWithPeriods] = None
        self.minUtilityRatio: float = 0.0

        self.startTimestamp: float = 0.0
        self.endTimestamp: float = 0.0
        self.candidatesCount: int = 0

        self.mapItemPeriod: Optional[Dict[int, int]] = None  # item -> bitmask
        self.periodUtilities: Optional[List[int]] = None
        self.mapItemExactEstUtility: Optional[Dict[int, Pair]] = None
        self.negativeItems: Optional[Set[int]] = None

        self.hashtable: Optional[HashTable] = None
        self.candidate1: List[int] = []
        self.candidate1_set: Set[int] = set()

        self.resultCount: int = 0
        self.writer = None

        self.DEBUG: bool = False

    def runAlgorithm(self, database: DatabaseWithPeriods, minUtilityRatio: float, output: str, periodCount: int) -> None:
        self.database = database
        self.minUtilityRatio = minUtilityRatio

        self.writer = open(output, "w", encoding="utf-8")

        MemoryLogger.getInstance().reset()
        self.startTimestamp = time.time()
        self.candidatesCount = 0

        self.hashtable = HashTable(10000)
        self.mapItemPeriod = database.getMapItemPeriod()
        self.negativeItems = database.getNegativeItems()
        self.periodUtilities = database.getPeriodUtilities()
        self.mapItemExactEstUtility = database.getMapItemExactEstUtility()

        # ====== Find promising 1-itemsets and output 1-item HOUIs ======
        # Important: Java removes unpromising items from database.getAllItems()
        items_to_check = list(self.mapItemExactEstUtility.items())
        for item, pair in items_to_check:
            periods_mask = self.mapItemPeriod.get(item, 0)
            is_promising_any = False
            sum_period_utility = 0

            for p in range(database.getPeriodCount()):
                if periods_mask & (1 << p):
                    period_util = self.periodUtilities[p]
                    est = pair.estimatedUtility[p] if pair.estimatedUtility else None
                    if (not is_promising_any) and est is not None:
                        if self._relative_utility(period_util, est) >= self.minUtilityRatio:
                            is_promising_any = True
                    sum_period_utility += period_util

            if not is_promising_any:
                database.getAllItems().discard(item)
            else:
                rel = self._relative_utility(sum_period_utility, pair.exactUtility)
                if rel >= self.minUtilityRatio:
                    self._writeOutItem(item, pair.exactUtility, rel)

        # candidate1 = all items still in mapItemExactEstUtility (Java does this),
        # but membership checks depend on "candidate1.contains". We'll keep only
        # those that remain in database.getAllItems() for correctness after pruning.
        self.candidate1 = sorted([i for i in self.mapItemExactEstUtility.keys() if i in database.getAllItems()])
        self.candidate1_set = set(self.candidate1)

        if len(self.candidate1) == 0:
            MemoryLogger.getInstance().checkMemory()
            self.endTimestamp = time.time()
            self.writer.close()
            return

        # ===== Remove unpromising items from transactions + adjust TU =====
        new_transactions: List[TransactionWithPeriod] = []
        for trans in database.getTransactions():
            new_items: List[ItemUtility] = []
            tu_pos = trans.transactionUtility
            for iu in trans.getItems():
                if iu.item in database.getAllItems():
                    new_items.append(iu)
                else:
                    # Java subtracts only positive utility from TU
                    if iu.utility > 0:
                        tu_pos -= iu.utility
            if new_items:
                new_transactions.append(TransactionWithPeriod(new_items, tu_pos, trans.getPeriod()))
        database.getTransactions().clear()
        database.getTransactions().extend(new_transactions)

        # ===== Sort transactions by (period, first item) =====
        database.getTransactions().sort(key=lambda t: (t.getPeriod(), t.getItems()[0].item))

        # ===== Compute (start,end) indices per period in sorted transaction list =====
        periodsStart, periodsEnd = self._compute_period_ranges(database.getTransactions(), database.getPeriodCount())

        # ===== Find 2-item candidates: build mapItemItemUtility =====
        mapItemItemUtility: Dict[int, Dict[int, Pair]] = {}

        for trans in database.getTransactions():
            period = trans.getPeriod()
            items = trans.getItems()
            for i in range(len(items)):
                itemI = items[i].item
                if itemI not in self.candidate1_set:
                    continue
                inner = mapItemItemUtility.get(itemI)
                if inner is None:
                    inner = {}
                    mapItemItemUtility[itemI] = inner

                for j in range(i + 1, len(items)):
                    itemJ = items[j].item
                    if itemJ not in self.candidate1_set:
                        continue
                    pairIJ = inner.get(itemJ)
                    if pairIJ is None:
                        pairIJ = Pair.with_periods(periodCount)
                        inner[itemJ] = pairIJ
                    pairIJ.exactUtility += (items[i].utility + items[j].utility)
                    if pairIJ.estimatedUtility[period] is None:
                        pairIJ.estimatedUtility[period] = trans.getTransactionUtility()
                    else:
                        pairIJ.estimatedUtility[period] += trans.getTransactionUtility()

        # ===== Remove unpromising 2-itemsets, output HOU 2-itemsets, build candidates2 list =====
        candidates2: List[ItemsetTP] = []
        for itemI, inner in list(mapItemItemUtility.items()):
            # iterate over copy for safe removal
            for itemJ, pairIJ in list(inner.items()):
                is_promising_any = False
                sum_period_utility = 0
                for p in range(len(pairIJ.estimatedUtility)):
                    if pairIJ.estimatedUtility[p] is not None:
                        if (not is_promising_any) and self._relative_utility(self.periodUtilities[p], pairIJ.estimatedUtility[p]) >= self.minUtilityRatio:
                            is_promising_any = True
                        sum_period_utility += self.periodUtilities[p]

                if not is_promising_any:
                    del inner[itemJ]
                    continue

                arr = [itemI, itemJ]
                candidates2.append(ItemsetTP(arr))
                self.candidatesCount += 1

                rel = self._relative_utility(sum_period_utility, pairIJ.exactUtility)
                if rel >= self.minUtilityRatio:
                    self._writeOut(arr, pairIJ.exactUtility, rel)

            if not inner:
                del mapItemItemUtility[itemI]

        if not candidates2:
            MemoryLogger.getInstance().checkMemory()
            self.endTimestamp = time.time()
            self.writer.close()
            return

        # Sort 2-candidates lexicographically by (first, second)
        candidates2.sort(key=lambda it: (it.items[0], it.items[1]))

        MemoryLogger.getInstance().checkMemory()

        # ===== For each period, mine higher-size candidates inside that period =====
        for current_period in range(database.getPeriodCount()):
            start = periodsStart[current_period]
            end = periodsEnd[current_period]
            if start is None or end is None:
                continue
            periodDB = database.getTransactions()[start : end + 1]
            periodUtility = self.periodUtilities[current_period]
            self.performMiningOnPeriod(periodDB, periodUtility, candidates2, current_period)

        MemoryLogger.getInstance().checkMemory()

        # ===== Final scan of hashtable to output itemsets whose relative utility over all on-shelf periods >= threshold =====
        assert self.hashtable is not None
        for bucket in self.hashtable.table:
            if bucket is None:
                continue
            for itemset in bucket:
                # Determine periods where itemset occurs: AND of item bitmasks
                periods_mask = self.mapItemPeriod.get(itemset.items[0], 0)
                for k in range(1, len(itemset.items)):
                    periods_mask &= self.mapItemPeriod.get(itemset.items[k], 0)

                exactUtility_total = 0
                sumPeriodUtility = 0

                # For each period in which itemset appears
                for p in range(database.getPeriodCount()):
                    if periods_mask & (1 << p):
                        sumPeriodUtility += self.periodUtilities[p]

                        # If already computed exact for this period, use it
                        found = False
                        if itemset.listPeriodUtility is not None:
                            for pu in itemset.listPeriodUtility:
                                if pu.period == p:
                                    exactUtility_total += pu.utility
                                    found = True
                                    break
                        if found:
                            continue

                        # Otherwise compute exact utility in that period by scanning its transactions
                        start = periodsStart[p]
                        end = periodsEnd[p]
                        if start is None or end is None:
                            continue
                        periodDB = database.getTransactions()[start : end + 1]
                        for trans in periodDB:
                            exactUtility_total += self.containsOrEquals(trans.getItems(), list(itemset.items))

                rel = self._relative_utility(sumPeriodUtility, exactUtility_total)
                if rel >= self.minUtilityRatio:
                    self._writeOut(list(itemset.items), exactUtility_total, rel)

        MemoryLogger.getInstance().checkMemory()
        self.endTimestamp = time.time()
        self.writer.close()

    # ----------------- core helpers -----------------
    @staticmethod
    def _compute_period_ranges(transactions: List[TransactionWithPeriod], period_count: int) -> Tuple[List[Optional[int]], List[Optional[int]]]:
        start = [None] * period_count
        end = [None] * period_count
        for idx, t in enumerate(transactions):
            p = t.getPeriod()
            if start[p] is None:
                start[p] = idx
            end[p] = idx
        return start, end

    def performMiningOnPeriod(self, database: List[TransactionWithPeriod], periodUtility: int, candidates2: List[ItemsetTP], period: int) -> None:
        MemoryLogger.getInstance().checkMemory()

        # Generate size-3 candidates from size-2 candidates (as in Java)
        candidatesSize3: List[List[int]] = []
        for i in range(len(candidates2)):
            a = candidates2[i].items
            for j in range(i + 1, len(candidates2)):
                b = candidates2[j].items
                if a[0] > b[0]:
                    break
                if a[0] == b[0]:
                    candidatesSize3.append([a[0], a[1], b[1]])

        MemoryLogger.getInstance().checkMemory()

        # Prune + store exact period utility into hashtable
        self.pruneCandidatesAndCalculateExactUtility(database, periodUtility, period, candidatesSize3)

        # Recursive generation of larger candidates
        previous = candidatesSize3
        while previous:
            nxt = self.generateCandidateSizeK(previous)
            self.pruneCandidatesAndCalculateExactUtility(database, periodUtility, period, nxt)
            previous = nxt

        MemoryLogger.getInstance().checkMemory()

    def pruneCandidatesAndCalculateExactUtility(
        self,
        database: List[TransactionWithPeriod],
        periodUtility: int,
        period: int,
        candidates: List[List[int]],
    ) -> None:
        assert self.hashtable is not None

        pruned: List[List[int]] = []
        for itemset in candidates:
            self.candidatesCount += 1

            estimatedUtility = 0
            exactUtility = 0

            for trans in database:
                # Major optimization from Java:
                if trans.getItems()[0].item > itemset[0]:
                    break

                util_in_trans = self.containsOrEquals(trans.getItems(), itemset)
                if util_in_trans > 0:
                    estimatedUtility += trans.transactionUtility
                    exactUtility += util_in_trans

            # prune if estimated relative utility too low
            if self._relative_utility(periodUtility, estimatedUtility) < self.minUtilityRatio:
                continue

            # keep candidate
            pruned.append(itemset)

            # if exact qualifies in this period, store in hashtable
            if self._relative_utility(periodUtility, exactUtility) >= self.minUtilityRatio:
                h = self.hashtable.hashCode(itemset)
                stored = self.hashtable.retrieveItemset(itemset, h)
                if stored is None:
                    stored = ItemsetTP(itemset)
                    self.hashtable.put(stored, h)
                stored.setPeriodUtility(period, exactUtility)

        # mutate input list in-place like Java iterator removals
        candidates[:] = pruned
        MemoryLogger.getInstance().checkMemory()

    @staticmethod
    def containsOrEquals(transaction_items: List[ItemUtility], items: List[int]) -> int:
        """
        Return the sum of utilities of 'items' in 'transaction_items' if contained; else 0.
        Assumes lexical order and uses early stopping like Java.
        """
        utility = 0
        for target in items:
            found = False
            for iu in transaction_items:
                if iu.item == target:
                    utility += iu.utility
                    found = True
                    break
                elif iu.item > target:
                    return 0
            if not found:
                return 0
        return utility

    @staticmethod
    def generateCandidateSizeK(levelK_1: List[List[int]]) -> List[List[int]]:
        candidatesK: List[List[int]] = []
        # join-step like Apriori, using lexical order
        for i in range(len(levelK_1)):
            itemset1 = levelK_1[i]
            for j in range(i + 1, len(levelK_1)):
                itemset2 = levelK_1[j]

                # compare prefixes
                can_join = True
                for k in range(len(itemset1)):
                    if k == len(itemset1) - 1:
                        if itemset1[k] >= itemset2[k]:
                            can_join = False
                            break
                    else:
                        if itemset1[k] < itemset2[k]:
                            # continue searching in j
                            can_join = None  # signal "continue j loop"
                            break
                        elif itemset1[k] > itemset2[k]:
                            can_join = False
                            break
                if can_join is None:
                    continue
                if not can_join:
                    # because of lexical order, stop searching for this i
                    break

                newItemset = itemset1[:] + [itemset2[-1]]
                candidatesK.append(newItemset)
        return candidatesK

    def _relative_utility(self, sumPeriodUtility: int, utility: float) -> float:
        if sumPeriodUtility == 0:
            return 0.0
        return float(utility) / abs(float(sumPeriodUtility))

    # ----------------- output -----------------
    def _writeOut(self, prefix: List[int], exactUtility: int, relativeUtility: float) -> None:
        line = " ".join(str(x) for x in prefix) + " "
        line += f"#UTIL: {exactUtility} #RUTIL: {relativeUtility}"
        self.writer.write(line + "\n")
        self.resultCount += 1

    def _writeOutItem(self, item: int, exactUtility: int, relativeUtility: float) -> None:
        # Write everything on ONE line (cleaner and matches other patterns)
        line = f"{item} #UTIL: {exactUtility} #RUTIL: {relativeUtility}"
        self.writer.write(line + "\n")
        self.resultCount += 1

    # ----------------- stats -----------------
    def printStats(self) -> None:
        assert self.database is not None
        print("=============  TS-HOUN ALGORITHM v2.02 - STATS =============")
        print(f" Transactions count from database : {self.database.size()}")
        print(f" Candidates count : {self.candidatesCount}")
        print(f" Memory : {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" HOU count : {self.resultCount}")
        print(f" Total time ~ {int((self.endTimestamp - self.startTimestamp) * 1000)} ms")
        print("===================================================")


# =========================================================
# Main (like MainTestTSHOUN_saveToFile.java)
# =========================================================
# ---------------- Main (Auto-run like Java) ----------------
def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT = os.path.join(BASE_DIR, "DB_FOSHU.txt")
    OUTPUT = os.path.join(BASE_DIR, "output_py.txt")
    MIN_RATIO = 0.8
    PERIODS = 3

    print("Running TS-HOUN (Python) with defaults:")
    print(" INPUT :", INPUT)
    print(" OUTPUT:", OUTPUT)
    print(" MIN_RATIO:", MIN_RATIO)
    print(" PERIODS:", PERIODS)

    db = DatabaseWithPeriods(PERIODS)
    db.loadFile(INPUT)

    algo = AlgoTSHoun()
    algo.runAlgorithm(db, MIN_RATIO, OUTPUT, PERIODS)
    algo.printStats()

if __name__ == "__main__":
    main()
