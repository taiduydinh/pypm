"""
HGB-ALL: High-Utility Association Rule Mining

Original authors: Jayakrushna Sahoo, Philippe Fournier-Viger
Paper: "An efficient approach for mining association rules from high utility
        itemsets" by Sahoo et al. (2015)

Usage:
    python hgb_all.py <input_file> <output_file> <min_utility> <min_conf>

Example:
    python hgb_all.py DB_Utility.txt output.txt 30 0.5
"""

import os
import time
import tracemalloc
from bisect import bisect_left
from typing import Dict, List, Optional, Tuple


# =============================================================================
# Element
# =============================================================================

class Element:
    """One entry in a utility list: (tid, iutils, rutils)."""

    __slots__ = ("tid", "iutils", "rutils")

    def __init__(self, tid: int, iutils: int, rutils: int):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


# =============================================================================
# UtilityList
# =============================================================================

class UtilityList:
    """Utility list for one item (or itemset extension)."""

    def __init__(self, item: int):
        self.item = item
        self.sum_iutils = 0
        self.sum_rutils = 0
        self.exutil = 0
        self.elements: List[Element] = []

    def add_element(self, e: Element):
        self.sum_iutils += e.iutils
        self.sum_rutils += e.rutils
        self.elements.append(e)

    def set_exutil(self, v: int):
        self.exutil = v


# =============================================================================
# Itemset
# =============================================================================

class Itemset:
    """
    An ordered list of items with per-item utilities, a total utility
    (acutility) and a support count.
    """

    def __init__(self,
                 items: Optional[List[int]] = None,
                 items_utilities: Optional[List[int]] = None,
                 transaction_utility: int = 0):
        self._items: List[int] = list(items) if items else []
        self._utils: List[int] = list(items_utilities) if items_utilities else []
        self.acutility: int = transaction_utility
        self.support: int = 0

    # ---- item access -------------------------------------------------------
    def add_item(self, v: int):
        self._items.append(v)

    def add_utility(self, v: int):
        self._utils.append(v)

    # alias used internally
    addutility = add_utility

    def get(self, i: int) -> int:
        return self._items[i]

    def get_items(self) -> List[int]:
        return self._items

    def get_items_utilities(self) -> List[int]:
        return self._utils

    def size(self) -> int:
        return len(self._items)

    def contains(self, item: int) -> bool:
        return item in self._items

    def contains1(self, item: int) -> int:
        """Return index of item or -1."""
        try:
            return self._items.index(item)
        except ValueError:
            return -1

    def sort(self):
        self._items.sort()

    # ---- equality / hashing ------------------------------------------------
    def is_equal_to(self, other: "Itemset") -> bool:
        return sorted(self._items) == sorted(other._items)

    def included_in(self, other: "Itemset") -> bool:
        return all(x in other._items for x in self._items)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Itemset):
            return False
        return sorted(self._items) == sorted(other._items)

    def __hash__(self):
        return hash(tuple(sorted(self._items)))

    def __repr__(self):
        return f"{self._items}:util={self.acutility}:sup={self.support}"

    # ---- cloning / set ops -------------------------------------------------
    def clone(self) -> "Itemset":
        c = Itemset()
        c._items = list(self._items)
        return c

    def clone_itemset_minus_an_itemset(self, to_remove: "Itemset") -> "Itemset":
        result = Itemset()
        for x in self._items:
            if not to_remove.contains(x):
                result.add_item(x)
        result.sort()
        return result

    def union(self, other: "Itemset") -> "Itemset":
        u = Itemset()
        u._items = list(self._items)
        for x in other._items:
            if x not in self._items:
                u.add_item(x)
        u.sort()
        return u

    def union_u(self, other: "Itemset") -> "Itemset":
        u = Itemset()
        u._items = list(self._items)
        u._utils = list(self._utils)
        for i, x in enumerate(other._items):
            if x not in self._items:
                u.add_item(x)
                u.add_utility(other._utils[i])
        u._bubble_sort()
        return u

    def _bubble_sort(self):
        n = len(self._items)
        for i in range(n):
            for j in range(n - 1, i, -1):
                if self._items[j] < self._items[j - 1]:
                    self._items[j], self._items[j - 1] = self._items[j - 1], self._items[j]
                    self._utils[j], self._utils[j - 1] = self._utils[j - 1], self._utils[j]


# =============================================================================
# HUTable
# =============================================================================

class HUTable:
    """Stores all high-utility itemsets organised by size (level)."""

    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.map_supp:   Dict[Itemset, int]  = {}
        self.map_key:    Dict[Itemset, bool] = {}
        self.map_closed: Dict[Itemset, bool] = {}

    def add_itemset(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        self.levels[itemset.size()].append(itemset)

    def get_level(self, i: int) -> Optional[List[Itemset]]:
        """Return levels[i] or None if it does not exist."""
        return self.levels[i] if i < len(self.levels) else None


# =============================================================================
# HUClosedTable
# =============================================================================

class HUClosedTable:
    """Stores closed high-utility itemsets and their generators."""

    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.map_generators: Dict[Itemset, List[Itemset]] = {}

    def add_closed(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        tmp = self.levels[itemset.size()]
        for e in tmp:
            if e.included_in(itemset):
                return
        tmp.append(itemset)


# =============================================================================
# Rule
# =============================================================================

class Rule:
    """A high-utility association rule with utility-confidence."""

    def __init__(self, antecedent: Itemset, consequent: Itemset,
                 utility: int, confidence: float,
                 parent: Itemset, ant_utility: int):
        self._ant = antecedent
        self._con = consequent
        self._utility = utility
        self._confidence = confidence
        self._parent = parent          # full itemset (antecedent ∪ consequent)
        self._ant_utility = ant_utility

    def get_antecedent(self)  -> Itemset: return self._ant
    def get_consequent(self)  -> Itemset: return self._con
    def get_utility(self)     -> int:     return self._utility
    def get_confidence(self)  -> float:   return self._confidence
    def get_parent(self)      -> Itemset: return self._parent
    def get_ant_utility(self) -> int:     return self._ant_utility

    def __repr__(self):
        return f"{self._ant.get_items()} ==> {self._con.get_items()}"


# =============================================================================
# Rules
# =============================================================================

class Rules:
    """Collection of rules organised by parent-itemset size (level)."""

    def __init__(self, name: str):
        self.name = name
        self.rules: List[List[Rule]] = []
        self._count = 0

    def add_rule(self, rule: Rule):
        sz = rule.get_parent().size()
        while len(self.rules) <= sz:
            self.rules.append([])
        self.rules[sz].append(rule)
        self._count += 1

    @property
    def count(self) -> int:
        return self._count


# =============================================================================
# MemoryLogger  (singleton)
# =============================================================================

class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self):
        self._max_mb: float = 0.0
        tracemalloc.start()

    @classmethod
    def get_instance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self):
        self._max_mb = 0.0
        tracemalloc.clear_traces()

    def check_memory(self) -> float:
        snap = tracemalloc.take_snapshot()
        mb = sum(s.size for s in snap.statistics("lineno")) / 1_048_576.0
        if mb > self._max_mb:
            self._max_mb = mb
        return mb

    def get_max_memory(self) -> float:
        return self._max_mb


# =============================================================================
# AlgoFHIM_and_HUCI
# =============================================================================

class AlgoFHIM_and_HUCI:
    """
    FHIM (algo=0) and HUCI-Miner (algo=1) for finding high-utility itemsets.
    """

    def __init__(self):
        self.max_memory:     float = 0.0
        self.start_ts:       float = 0.0
        self.end_ts:         float = 0.0
        self.hui:            int   = 0
        self.candidate:      int   = 0
        self.chui:           int   = 0
        self.ghui:           int   = 0
        self.maxlength:      int   = 0
        self.min_utility:    int   = 0

        self._algo:          int   = 0
        self._output_path:   Optional[str] = None

        self._table_hui:     Optional[HUTable]       = None
        self._table_huci:    Optional[HUClosedTable]  = None
        self._HG:            List[Itemset]            = []

        self.map_fmap:       Dict[int, Dict[int, int]]                         = {}
        self.map_item_ul:    Dict[int, UtilityList]                            = {}
        self.map_ll_fmap:    Dict[int, Optional[Dict[int, Dict[int, int]]]]    = {}

        self.no_closed:      bool = False
        self.no_generators:  bool = False

    # ---------------------------------------------------------------- public
    def run_fhim(self, input_path: str, output_path: Optional[str],
                 min_util: int):
        self._run(input_path, output_path, min_util, 0)

    def run_huci_miner(self, input_path: str, output_path: Optional[str],
                       min_util: int) -> HUClosedTable:
        return self._run(input_path, output_path, min_util, 1)

    def get_table_hu(self) -> HUTable:
        return self._table_hui

    # ---------------------------------------------------------------- core
    def _run(self, input_path: str, output_path: Optional[str],
             min_utility: int, alg: int) -> HUClosedTable:

        self.max_memory = self.hui = self.candidate = 0
        self.chui = self.ghui = self.maxlength = 0
        self.start_ts = time.time()
        self._output_path = output_path
        self._algo = alg

        mem = MemoryLogger.get_instance()
        mem.reset()

        self.map_fmap     = {}
        self._HG          = []
        self._table_hui   = HUTable()
        self._table_huci  = HUClosedTable()

        # ---- Pass 1: TWU ------------------------------------------------
        map_twu: Dict[int, int] = {}
        try:
            with open(input_path, "r") as fh:
                for raw in fh:
                    line = raw.strip()
                    if not line or line[0] in "#%@":
                        continue
                    parts = line.split(":")
                    trans_util = int(parts[1])
                    for tok in parts[0].split():
                        it = int(tok)
                        map_twu[it] = map_twu.get(it, 0) + trans_util
        except (IOError, ValueError):
            pass

        self.min_utility = min_utility
        print(f"Absolute utility threshold = {self.min_utility}")

        # ---- Build utility lists ----------------------------------------
        list_uls: List[UtilityList] = []
        self.map_item_ul = {}
        for it, twu in map_twu.items():
            if twu >= min_utility:
                ul = UtilityList(it)
                self.map_item_ul[it] = ul
                list_uls.append(ul)

        list_uls.sort(key=lambda u: (map_twu[u.item], u.item))

        # ---- Pass 2: fill utility lists + FMAP --------------------------
        try:
            with open(input_path, "r") as fh:
                tid = 0
                for raw in fh:
                    line = raw.strip()
                    if not line or line[0] in "#%@":
                        continue
                    parts = line.split(":")
                    items_tok = parts[0].split()
                    util_tok  = parts[2].split()

                    rem = 0
                    new_twu = 0
                    revised: List[Tuple[int, int]] = []   # (item, utility)

                    for i, tok in enumerate(items_tok):
                        it  = int(tok)
                        utl = int(util_tok[i])
                        if map_twu.get(it, 0) >= min_utility:
                            revised.append((it, utl))
                            rem     += utl
                            new_twu += utl

                    revised.sort(key=lambda p: (map_twu[p[0]], p[0]))

                    for i, (it, utl) in enumerate(revised):
                        rem -= utl
                        self.map_item_ul[it].add_element(Element(tid, utl, rem))

                        # FMAP
                        fmap_it = self.map_fmap.setdefault(it, {})
                        for j in range(i + 1, len(revised)):
                            nb = revised[j][0]
                            fmap_it[nb] = fmap_it.get(nb, 0) + new_twu

                    tid += 1
        except (IOError, ValueError):
            pass

        mem.check_memory()

        # ---- Mine HUIs --------------------------------------------------
        self.map_ll_fmap = {}
        prefix = Itemset()

        for i, X in enumerate(list_uls):
            if X.sum_iutils >= min_utility:
                self._store(prefix, X)

            if X.sum_iutils + X.sum_rutils >= min_utility:
                ex_uls: List[UtilityList] = []
                for j in range(i + 1, len(list_uls)):
                    Y = list_uls[j]
                    m = self.map_fmap.get(X.item)
                    if m is not None:
                        t = m.get(Y.item)
                        if t is not None and t < min_utility:
                            continue
                    self.candidate += 1
                    ex_uls.append(self._construct(None, X, Y))

                np_ = prefix.clone()
                np_.add_item(X.item)
                if self.map_ll_fmap.get(X.item) is None:
                    self.map_ll_fmap[X.item] = {}
                self._hui_miner(X.item, True, np_, X, ex_uls)
                self.map_ll_fmap[X.item] = None

        # ---- HUCI post-process ------------------------------------------
        if self._algo == 1:
            self._huci_miner()

        mem.check_memory()
        self.max_memory = mem.get_max_memory()
        self.end_ts = time.time()

        # ---- Write output -----------------------------------------------
        if output_path is not None:
            lines: List[str] = []
            for level in self._table_huci.levels:
                for iset in level:
                    if not self.no_generators and not self.no_closed:
                        lines.append("CLOSED: ")
                    if not self.no_closed:
                        lines.append(self._fmt_itemset(iset))
                    gens = self._table_huci.map_generators.get(iset)
                    if not self.no_generators and gens:
                        if not self.no_closed:
                            lines.append("GENERATOR: ")
                        for g in gens:
                            lines.append(self._fmt_itemset(g))
                            self.ghui += 1
            with open(output_path, "w") as fh:
                fh.write("\n".join(lines))

        return self._table_huci

    # ---------------------------------------------------------------- store
    def _store(self, prefix: Itemset, X: UtilityList):
        self.hui += 1
        if self._algo == 1:
            np_ = prefix.clone()
            np_.add_item(X.item)
            k1 = np_.size()
            if self.maxlength < k1:
                self.maxlength = k1
            np_.acutility = X.sum_iutils
            np_.support    = len(X.elements)
            np_.sort()
            self._util_unit_array(np_, X)
            self._table_hui.add_itemset(np_)
            self._table_hui.map_key[np_]    = True
            self._table_hui.map_supp[np_]   = np_.support
            self._table_hui.map_closed[np_] = True

        elif self._algo == 0 and self._output_path is not None:
            temp = sorted(list(prefix.get_items()) + [X.item])
            if self.maxlength < len(temp):
                self.maxlength = len(temp)
            item_str = " ".join(str(x) for x in temp)
            line = f"{item_str}  #UTIL: {X.sum_iutils} #SUP: {len(X.elements)}"
            with open(self._output_path, "a") as fh:
                fh.write(line + "\n")

    # ---------------------------------------------------------------- huci
    def _huci_miner(self):
        for itr in range(2, self.maxlength + 1):
            lv_cur  = self._table_hui.get_level(itr)
            lv_prev = self._table_hui.get_level(itr - 1)

            if lv_cur is not None and lv_prev is not None:
                for l in lv_cur:
                    for s in self._subset(lv_prev, l):
                        if s.support == l.support:
                            self._table_hui.map_closed[s] = False
                            self._table_hui.map_key[l]    = False
                self._process_prev_level(lv_prev)

            elif lv_prev is not None:
                self._process_prev_level(lv_prev)

        lv_max = self._table_hui.get_level(self.maxlength)
        if lv_max is not None:
            for l in lv_max:
                if self._table_hui.map_closed.get(l, False):
                    self._table_huci.add_closed(l)
                    self.chui += 1
                    s_list = self._subset(self._HG, l)
                    self._table_huci.map_generators[l] = s_list
                    for s in s_list:
                        self._HG.remove(s)

    def _process_prev_level(self, lv_prev: List[Itemset]):
        for l in lv_prev:
            if self._table_hui.map_closed.get(l, False):
                self._table_huci.add_closed(l)
                self.chui += 1
                s_list = self._subset(self._HG, l)
                self._table_huci.map_generators[l] = s_list
                for s in s_list:
                    self._HG.remove(s)
            if (self._table_hui.map_key.get(l, False)
                    and not self._table_hui.map_closed.get(l, True)):
                self._HG.append(l)

    # ---------------------------------------------------------------- util arr
    def _util_unit_array(self, l: Itemset, Z: UtilityList):
        for i in range(l.size()):
            ite = l.get(i)
            jk  = self.map_item_ul.get(ite)
            v   = 0
            for e in Z.elements:
                ey = self._find_tid(jk, e.tid)
                if ey is not None:
                    v += ey.iutils
            l.addutility(v)

    # ---------------------------------------------------------------- hui miner
    def _hui_miner(self, k: int, ft: bool,
                   prefix: Itemset, p_ul: UtilityList,
                   uls: List[UtilityList]):
        l_map = self.map_ll_fmap.get(k)
        for i in range(len(uls) - 1, -1, -1):
            X = uls[i]
            if X.sum_iutils >= self.min_utility:
                self._store(prefix, X)
            if X.sum_iutils + X.sum_rutils >= self.min_utility:
                ex_uls: List[UtilityList] = []
                for j in range(i + 1, len(uls)):
                    Y = uls[j]
                    if Y.exutil >= self.min_utility:
                        if prefix.size() < 2:
                            m = self.map_fmap.get(X.item)
                            if m is not None:
                                t = m.get(Y.item)
                                if t is not None and t < self.min_utility:
                                    continue
                        elif Y.exutil < self.min_utility:
                            continue
                        else:
                            if l_map is not None:
                                m = l_map.get(X.item)
                                if m is not None:
                                    t = m.get(Y.item)
                                    if t is not None and t < self.min_utility:
                                        continue
                        self.candidate += 1
                        if ft and prefix.size() == 1:
                            ex_uls.append(self._construct_l(p_ul, X, Y, k))
                        else:
                            ex_uls.append(self._construct(p_ul, X, Y))

                np_ = prefix.clone()
                np_.add_item(X.item)
                self._hui_miner(k, True, np_, X, ex_uls)

    # ---------------------------------------------------------------- construct
    def _construct(self, P: Optional[UtilityList],
                   px: UtilityList, py: UtilityList) -> UtilityList:
        pxy = UtilityList(py.item)
        new_twu = 0
        for ex in px.elements:
            ey = self._find_tid(py, ex.tid)
            if ey is None:
                continue
            if P is None:
                pxy.add_element(Element(ex.tid, ex.iutils + ey.iutils, ey.rutils))
                new_twu += ex.iutils + ex.rutils
            else:
                e = self._find_tid(P, ex.tid)
                if e is not None:
                    pxy.add_element(Element(ex.tid,
                                            ex.iutils + ey.iutils - e.iutils,
                                            ey.rutils))
                    new_twu += ex.iutils + ex.rutils
        pxy.set_exutil(new_twu)
        return pxy

    def _construct_l(self, P: Optional[UtilityList],
                     px: UtilityList, py: UtilityList, k: int) -> UtilityList:
        pxy = UtilityList(py.item)
        new_twu = 0
        new_ex  = 0
        for ex in px.elements:
            ey = self._find_tid(py, ex.tid)
            if ey is None:
                continue
            if P is None:
                pxy.add_element(Element(ex.tid, ex.iutils + ey.iutils, ey.rutils))
            else:
                e = self._find_tid(P, ex.tid)
                if e is not None:
                    pxy.add_element(Element(ex.tid,
                                            ex.iutils + ey.iutils - e.iutils,
                                            ey.rutils))
                    new_twu += e.iutils  + e.rutils
                    new_ex  += ex.iutils + ex.rutils

        # LLFMAP update
        if self.map_ll_fmap.get(k) is None:
            self.map_ll_fmap[k] = {}
        lm = self.map_ll_fmap[k]
        if lm.get(px.item) is None:
            lm[px.item] = {}
        lm[px.item][py.item] = new_twu
        pxy.set_exutil(new_ex)
        return pxy

    # ---------------------------------------------------------------- binary search
    def _find_tid(self, ulist: UtilityList, tid: int) -> Optional[Element]:
        lst = ulist.elements
        lo, hi = 0, len(lst) - 1
        while lo <= hi:
            mid = (lo + hi) >> 1
            t = lst[mid].tid
            if t < tid:
                lo = mid + 1
            elif t > tid:
                hi = mid - 1
            else:
                return lst[mid]
        return None

    # ---------------------------------------------------------------- subset
    def _subset(self, s: List[Itemset], l: Itemset) -> List[Itemset]:
        return [iset for iset in s
                if all(l.contains(x) for x in iset.get_items())]

    # ---------------------------------------------------------------- format
    @staticmethod
    def _fmt_itemset(iset: Itemset) -> str:
        items = iset.get_items()
        s = " ".join(str(x) for x in items)
        return f"{s} #UTIL: {iset.acutility} #SUP: {iset.support}"

    # ---------------------------------------------------------------- stats
    def print_stats(self):
        algo_name = "FHIM" if self._algo == 0 else "HUCI-Miner"
        print(f"=============  {algo_name} ALGORITHM - STATS =============")
        elapsed = int((self.end_ts - self.start_ts) * 1000)
        print(f" Total time ~ {elapsed} ms")
        print(f" Memory ~ {self.max_memory} MB")
        print(f" Candidate count : {self.candidate}")
        print(f" High-utility itemsets count : {self.hui}")
        if self._algo == 1 and not self.no_closed:
            print(f" Closed High-utility itemsets count : {self.chui}")
        if self._algo == 1 and not self.no_generators and self._output_path is not None:
            print(f" Generator High-utility itemsets count : {self.ghui}")
        print("===================================================")


# =============================================================================
# AlgoHGBAll
# =============================================================================

class AlgoHGBAll:
    """
    Derives all high-utility association rules from a HUTable.
    Implements the HGB-ALL algorithm.
    """

    def __init__(self):
        self._rules:       Optional[Rules] = None
        self._runtime_ms:  float = 0.0
        self._max_memory:  float = 0.0
        self._rule_count:  int   = 0

    def run_algorithm(self, patterns: HUTable,
                      min_conf: float, min_utility: int) -> Rules:
        mem = MemoryLogger.get_instance()
        mem.reset()
        start = time.time()

        self._rule_count = 0
        self._rules = Rules("All high utility association rules")
        levels = patterns.levels

        # For each HUI lk of size >= 2 (levels index k >= 1)
        for k in range(1, len(levels)):
            for lk in levels[k]:
                # For each HUI at a larger size j > k
                for j in range(k + 1, len(levels)):
                    # Supersets of lk at level j
                    lk_items = set(lk.get_items())
                    for hm_p_1 in levels[j]:
                        if not lk_items.issubset(set(hm_p_1.get_items())):
                            continue

                        # Consequent = hm_p_1 minus lk
                        consequent = hm_p_1.clone_itemset_minus_an_itemset(lk)

                        # Shared utility: sum utilities of lk items inside hm_p_1
                        share = 0
                        for pos in range(hm_p_1.size()):
                            if lk.contains1(hm_p_1.get(pos)) != -1:
                                share += hm_p_1.get_items_utilities()[pos]

                        conf = share / lk.acutility if lk.acutility != 0 else 0.0
                        if conf >= min_conf:
                            rule = Rule(
                                antecedent   = lk,
                                consequent   = consequent,
                                utility      = hm_p_1.acutility,
                                confidence   = conf,
                                parent       = hm_p_1,
                                ant_utility  = lk.acutility,
                            )
                            self._rules.add_rule(rule)
                            self._rule_count += 1

        mem.check_memory()
        self._runtime_ms = (time.time() - start) * 1000
        self._max_memory = mem.get_max_memory()
        print(f"Total number of HARs {self._rule_count}")
        return self._rules

    def write_rules_to_file(self, output_path: str):
        """Write rules in the same format as the Java implementation."""
        lines: List[str] = []
        for rule_list in self._rules.rules:
            for rule in rule_list:
                ant_str = " ".join(str(x) for x in rule.get_antecedent().get_items())
                con_str = " ".join(str(x) for x in rule.get_consequent().get_items())
                lines.append(
                    f"{ant_str} ==> {con_str}"
                    f" #UTIL: {rule.get_utility()}"
                    f" #AUTIL: {rule.get_ant_utility()}"
                    f" #UCONF: {rule.get_confidence()}"
                )
        with open(output_path, "w") as fh:
            fh.write("\n".join(lines))

    def print_stats(self):
        print("=============  HGB-ALL ALGORITHM - STATS =============")
        print(f" Total time ~ {int(self._runtime_ms)} ms")
        print(f" Memory ~ {self._max_memory} MB")
        print(f" High-utility association rule count : {self._rule_count}")
        print("===================================================")



# =============================================================================
# Java-style method aliases  (so main() matches the Java test class exactly)
# =============================================================================

# AlgoFHIM_and_HUCI
AlgoFHIM_and_HUCI.runAlgorithmFHIM       = AlgoFHIM_and_HUCI.run_fhim
AlgoFHIM_and_HUCI.runAlgorithmHUCIMiner  = AlgoFHIM_and_HUCI.run_huci_miner
AlgoFHIM_and_HUCI.getTableHU             = AlgoFHIM_and_HUCI.get_table_hu
AlgoFHIM_and_HUCI.printStats             = AlgoFHIM_and_HUCI.print_stats

# AlgoHGBAll
AlgoHGBAll.runAlgorithm      = AlgoHGBAll.run_algorithm
AlgoHGBAll.writeRulesToFile  = AlgoHGBAll.write_rules_to_file
AlgoHGBAll.printStats        = AlgoHGBAll.print_stats


# =============================================================================
# Entry point
# =============================================================================

def main():
    # --------------------------------------------------
    # Set parameters directly here
    # --------------------------------------------------
    input_path  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DB_Utility.txt")
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_py.txt")
    min_utility = 30
    minconf     = 0.5
    # --------------------------------------------------

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Step 1: HUCI-Miner
    huci = AlgoFHIM_and_HUCI()
    results = huci.runAlgorithmHUCIMiner(input_path, None, min_utility)
    huci.printStats()

    # Step 2: HGB-ALL
    algo = AlgoHGBAll()
    algo.runAlgorithm(huci.getTableHU(), minconf, min_utility)
    algo.writeRulesToFile(output_path)
    algo.printStats()

if __name__ == "__main__":
    main()