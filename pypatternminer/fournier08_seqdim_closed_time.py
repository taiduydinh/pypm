#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
(Closed) Multi-dimensional Sequential Patterns Mining with Time Constraints
https://www.philippe-fournier-viger.com/spmf/ClosedMultiDimensional_SequentialPatternsWithTime.php

Python port of AlgoFournierViger08 + AlgoSeqDim + AlgoDim from SPMF.

The algorithm requires the following parameters:
  minsup             : minimum support (0 < minsup <= 1), e.g. 0.5 for 50%
  minInterval   (C1) : minimum time gap between adjacent itemsets in a pattern
  maxInterval   (C2) : maximum time gap between adjacent itemsets in a pattern
  minWholeInterval (C3): minimum total time span of a sequence pattern
  maxWholeInterval (C4): maximum total time span of a sequence pattern

Run: press the Play button in VS Code (or python seqdim_closed_mdspm.py)
"""

import math
import time
import os
import subprocess
import tempfile
from itertools import combinations, product

# ====================================================================
#              CONFIGURATION — CHANGE THESE TO TEST
# ====================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(script_dir, "ContextMDSequence.txt")
OUTPUT_FILE = os.path.join(script_dir, "outputs.txt")

# Minimum support threshold (e.g. 0.5 = 50%, 0.7 = 70%)
MIN_SUPPORT = 0.5

# Time-gap constraints between adjacent itemsets (C1/C2)
MIN_INTERVAL = 0
MAX_INTERVAL = 1000       # use a large number for "no constraint"

# Whole-sequence time-span constraints (C3/C4)
MIN_WHOLE_INTERVAL = 0
MAX_WHOLE_INTERVAL = 1000  # use a large number for "no constraint"

# Compare with a Java output file? Set to None to skip, or "output.txt" to compare,
# or "--run-java" to compile and run Java automatically.
COMPARE_MODE = None

# ====================================================================


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class MemoryLogger:
    _instance = None

    def __init__(self):
        self.maxMemory = 0.0

    @staticmethod
    def getInstance():
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self):
        pass

    def getMaxMemory(self):
        return self.maxMemory


class ItemSimple:
    """Equivalent to ItemSimple.java"""
    def __init__(self, item_id: int):
        self._id = int(item_id)

    def getId(self):
        return self._id


class Itemset:
    """Equivalent to Itemset.java"""
    def __init__(self, items, timestamp: int):
        self._items = items
        self._timestamp = int(timestamp)

    def getItems(self):
        return self._items

    def getTimestamp(self):
        return self._timestamp


class Sequence:
    """Equivalent to Sequence.java"""
    def __init__(self, sid: int):
        self.sid = int(sid)
        self.itemsets = []
        self._sids = set()

    def addItemset(self, it: Itemset):
        self.itemsets.append(it)

    def getItemsets(self):
        return self.itemsets

    def size(self):
        return len(self.itemsets)

    def setSequencesID(self, sids):
        self._sids = set(sids)

    def getAbsoluteSupport(self):
        return len(self._sids)

    def toStringShort(self):
        out = []
        for it in self.itemsets:
            out.append("{t=" + str(it.getTimestamp()) + ", ")
            for x in it.getItems():
                out.append(str(x.getId()) + " ")
            out.append("}")
        out.append("     ")
        return "".join(out)

    def strictlyContains(self, other) -> int:
        """
        Returns:
          0 — this does NOT contain other
          1 — this STRICTLY contains other (this is more general, same support not checked here)
          2 — this and other are EQUAL as sequences
        """
        def helper(seq_a, seq_b, index_a, index_b, prev_ts_a, prev_ts_b):
            if index_a == seq_a.size():
                return 0
            if seq_a.size() - index_a < seq_b.size() - index_b:
                return 0
            return_value = 0
            for i in range(index_a, seq_a.size()):
                interval1 = seq_a.getItemsets()[i].getTimestamp() - prev_ts_a
                interval2 = seq_b.getItemsets()[index_b].getTimestamp() - prev_ts_b
                items_a = [x.getId() for x in seq_a.getItemsets()[i].getItems()]
                items_b = [x.getId() for x in seq_b.getItemsets()[index_b].getItems()]
                if set(items_a).issuperset(set(items_b)) and interval1 == interval2:
                    same_size = len(items_a) == len(items_b)
                    if index_b == seq_b.size() - 1:
                        if same_size:
                            return 2
                        return_value = 1
                    else:
                        res = helper(seq_a, seq_b, i + 1, index_b + 1,
                                     seq_a.getItemsets()[i].getTimestamp(),
                                     seq_b.getItemsets()[index_b].getTimestamp())
                        if res == 2 and same_size:
                            return 2
                        elif res != 0:
                            return_value = 1
            return return_value

        retour = helper(self, other, 0, 0, 0, 0)
        if retour == 2:
            return 2 if self.size() == other.size() else 1
        return retour


class SequenceDatabase:
    """Equivalent to SequenceDatabase.java"""
    def __init__(self):
        self.sequences = []

    def addSequence(self, s: Sequence):
        self.sequences.append(s)

    def getSequences(self):
        return self.sequences

    def size(self):
        return len(self.sequences)


class MDPattern:
    """Equivalent to MDPattern.java"""
    WILDCARD = -1

    def __init__(self):
        self.values = []
        self.sids = set()

    def add(self, v: int):
        self.values.append(int(v))

    def size(self):
        return len(self.values)

    def getValue(self, i: int):
        return self.values[i]

    def setPatternsIDList(self, sids):
        self.sids = set(sids)

    def getAbsoluteSupport(self):
        return len(self.sids)

    def toStringShort(self):
        out = ["[ "]
        for v in self.values:
            out.append("* " if v == MDPattern.WILDCARD else str(v) + " ")
        out.append("]")
        return "".join(out)

    def isAllWildcards(self):
        return all(v == MDPattern.WILDCARD for v in self.values)

    def strictlyContains(self, other) -> int:
        """
        Returns:
          0 — this does NOT contain other
          1 — this STRICTLY contains other (more general, same support)
          2 — this and other are EQUAL
        """
        if self.getAbsoluteSupport() != other.getAbsoluteSupport():
            return 0
        allthesame = True
        for i in range(len(self.values)):
            sv = self.values[i]
            ov = other.getValue(i)
            if sv != ov:
                allthesame = False
            if sv != MDPattern.WILDCARD and sv != ov:
                return 0
        return 2 if allthesame else 1


class MDSequence:
    """Equivalent to MDSequence.java"""
    def __init__(self, md: MDPattern, seq: Sequence):
        self.md = md
        self.seq = seq
        self.support = 0

    def setSupport(self, s: int):
        self.support = int(s)

    def getAbsoluteSupport(self):
        return self.support

    def contains(self, other) -> bool:
        """True if self contains other (both MD and sequence part)."""
        return (self.md.strictlyContains(other.md) != 0 and
                self.seq.strictlyContains(other.seq) != 0)


class MDSequenceDatabase:
    """Equivalent to MDSequenceDatabase.java"""
    def __init__(self):
        self.mdseqs = []
        self.mdpats = []
        self.seqdb = SequenceDatabase()

    def size(self):
        return len(self.mdseqs)

    def loadFile(self, path: str):
        sid = 0
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                t = line.strip().split()
                if not t:
                    continue
                i = 0
                md = MDPattern()
                while i < len(t) and t[i] != "-3":
                    md.add(MDPattern.WILDCARD if t[i] == "*" else int(t[i]))
                    i += 1
                if i >= len(t) or t[i] != "-3":
                    continue
                i += 1
                seq = Sequence(sid)
                cur_items = []
                cur_t = None
                while i < len(t):
                    tok = t[i]
                    if tok.startswith("<") and tok.endswith(">"):
                        cur_t = int(tok[1:-1])
                        cur_items = []
                    elif tok == "-1":
                        if cur_t is not None:
                            seq.addItemset(
                                Itemset([ItemSimple(x) for x in cur_items], cur_t))
                    elif tok == "-2":
                        break
                    else:
                        cur_items.append(int(tok))
                    i += 1
                self.mdpats.append(md)
                self.seqdb.addSequence(seq)
                self.mdseqs.append((md, seq))
                sid += 1


# ---------------------------------------------------------------------------
# AlgoDim — closed multi-dimensional pattern mining
# ---------------------------------------------------------------------------

class AlgoDim:
    """
    Equivalent to AlgoDim.java (AprioriClose branch, findClosedPatterns=True).

    Java encoding:
      Every dimension value — including wildcards (-1) — is mapped to a unique
      item ID.  AprioriClose then finds closed frequent itemsets over those items.
      After conversion back to MD patterns the all-wildcard pattern (empty itemset)
      is added only when the maximum support among closed frequent itemsets is
      strictly less than the projected DB size (mirroring the CHARM branch check
      `if (maxSupport < contextCharm.size())`).

    The caller (AlgoSeqDim) is responsible for skipping all-wildcard patterns
    when writing the final output.
    """

    def runAlgorithm(self, mdpatterns, minsup: float):
        if not mdpatterns:
            return []

        n = len(mdpatterns)
        minsupp_abs = max(1, int(math.ceil(minsup * n)))
        dims = mdpatterns[0].size()

        # Build one transaction per MD pattern.
        # Every (dim, value) pair — including wildcards — is treated as an item.
        transactions = [
            frozenset((dim, pat.getValue(dim)) for dim in range(dims))
            for pat in mdpatterns
        ]

        # Compute per-item support
        all_items = set()
        for txn in transactions:
            all_items.update(txn)

        item_sids = {
            item: frozenset(i for i, txn in enumerate(transactions) if item in txn)
            for item in all_items
        }
        frequent_1 = {
            frozenset([item]): sids
            for item, sids in item_sids.items()
            if len(sids) >= minsupp_abs
        }

        # Generate all frequent itemsets (Apriori level-wise)
        all_frequent = dict(frequent_1)
        prev_level = list(frequent_1.keys())
        while prev_level:
            next_level = []
            for i, c1 in enumerate(prev_level):
                for c2 in prev_level[i + 1:]:
                    union = c1 | c2
                    if len(union) != len(c1) + 1:
                        continue
                    if union in all_frequent:
                        continue
                    # Support = intersection of supports of all items
                    sids = None
                    for item in union:
                        s = item_sids.get(item, frozenset())
                        sids = s if sids is None else sids & s
                    if sids and len(sids) >= minsupp_abs:
                        all_frequent[union] = sids
                        next_level.append(union)
            prev_level = next_level

        # Keep only closed itemsets
        freq_list = list(all_frequent.items())
        closed_itemsets = {}
        for cand, sids in freq_list:
            is_closed = True
            for other_cand, other_sids in freq_list:
                if other_cand == cand or len(other_cand) <= len(cand):
                    continue
                if sids == other_sids and cand.issubset(other_cand):
                    is_closed = False
                    break
            if is_closed:
                closed_itemsets[cand] = sids

        # Add all-wildcard (empty itemset) when maxSupport < n
        # (mirrors Java CHARM branch: `if (maxSupport < contextCharm.size())`)
        max_support = max((len(s) for s in closed_itemsets.values()), default=0)
        if max_support < n:
            closed_itemsets[frozenset()] = frozenset(range(n))

        # Convert closed itemsets back to MDPatterns
        result = []
        for itemset, sids in closed_itemsets.items():
            pat = MDPattern()
            for _ in range(dims):
                pat.add(MDPattern.WILDCARD)
            for (dim, value) in itemset:
                # Wildcard items keep the dimension as wildcard
                if value != MDPattern.WILDCARD:
                    pat.values[dim] = value
            pat.setPatternsIDList(sids)
            result.append(pat)

        result.sort(key=lambda x: (-sum(v != MDPattern.WILDCARD for v in x.values),
                                   x.toStringShort()))
        return result


# ---------------------------------------------------------------------------
# AlgoSeq — closed sequential pattern mining with time constraints
# ---------------------------------------------------------------------------

class AlgoSeq:
    """
    Equivalent to AlgoFournierViger08 (the closed sequential pattern mining part).

    Time constraints applied during enumeration:
      C1 (minInterval)       : minimum gap between adjacent itemsets
      C2 (maxInterval)       : maximum gap between adjacent itemsets
      C3 (minWholeInterval)  : minimum total time span of the pattern
      C4 (maxWholeInterval)  : maximum total time span of the pattern

    A gap of 0 (same timestamp position) is always allowed for C1/C2.
    """

    def __init__(self, min_interval=0, max_interval=float('inf'),
                 min_whole_interval=0, max_whole_interval=float('inf')):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.min_whole_interval = min_whole_interval
        self.max_whole_interval = max_whole_interval

    def _gap_ok(self, gap: int) -> bool:
        if gap == 0:
            return True
        return self.min_interval <= gap <= self.max_interval

    def _whole_ok(self, total_time: int) -> bool:
        return self.min_whole_interval <= total_time <= self.max_whole_interval

    def runAlgorithm(self, seqdb: SequenceDatabase, minsup: float):
        minsupp_abs = max(1, int(math.ceil(minsup * seqdb.size())))

        # Enumerate all valid subsequences across the database
        patterns = {}
        for seq in seqdb.getSequences():
            for rep in self._enumerate_valid_subsequences(seq):
                patterns.setdefault(rep, set()).add(seq.sid)

        # Keep only frequent ones
        frequent_seqs = []
        for rep, sids in patterns.items():
            if len(sids) >= minsupp_abs:
                s = self._rep_to_seq(rep)
                s.setSequencesID(sids)
                frequent_seqs.append(s)

        # Keep only closed ones (BIDE+ semantics)
        closed = []
        for p in frequent_seqs:
            is_closed = True
            for q in frequent_seqs:
                if p is q:
                    continue
                if p.getAbsoluteSupport() != q.getAbsoluteSupport():
                    continue
                if self._is_closed_by(q, p):
                    is_closed = False
                    break
            if is_closed:
                closed.append(p)

        closed.sort(key=lambda x: (self._item_count(x), x.toStringShort()))
        return closed

    def _item_count(self, seq: Sequence) -> int:
        return sum(len(it.getItems()) for it in seq.getItemsets())

    def _is_closed_by(self, super_seq: Sequence, sub_seq: Sequence) -> bool:
        """True if super_seq strictly dominates sub_seq (same support, more items)."""
        if super_seq.toStringShort() == sub_seq.toStringShort():
            return False
        if not self._seq_contains(super_seq, sub_seq):
            return False
        return (self._item_count(super_seq) > self._item_count(sub_seq) or
                super_seq.size() > sub_seq.size())

    def _seq_contains(self, super_seq: Sequence, sub_seq: Sequence) -> bool:
        """True if super_seq contains sub_seq as a time-aligned subsequence."""
        sup_its = super_seq.getItemsets()
        sub_its = sub_seq.getItemsets()
        if not sub_its:
            return True

        def helper(si, bi, prev_s_ts, prev_b_ts):
            if bi == len(sub_its):
                return True
            tgt = sub_its[bi]
            tgt_items = {x.getId() for x in tgt.getItems()}
            tgt_gap = tgt.getTimestamp() - prev_b_ts
            for idx in range(si, len(sup_its)):
                src = sup_its[idx]
                src_items = {x.getId() for x in src.getItems()}
                src_gap = src.getTimestamp() - prev_s_ts
                if src_gap == tgt_gap and src_items.issuperset(tgt_items):
                    if helper(idx + 1, bi + 1,
                              src.getTimestamp(), tgt.getTimestamp()):
                        return True
            return False

        first_b = sub_its[0]
        first_b_items = {x.getId() for x in first_b.getItems()}
        for idx, first_s in enumerate(sup_its):
            if {x.getId() for x in first_s.getItems()}.issuperset(first_b_items):
                if helper(idx + 1, 1, first_s.getTimestamp(), first_b.getTimestamp()):
                    return True
        return False

    def _enumerate_valid_subsequences(self, seq: Sequence):
        """
        Enumerate all subsequences satisfying C1–C4.
        Each subsequence is a tuple of (relative_timestamp, sorted_items_tuple).
        """
        its = seq.getItemsets()
        results = set()

        for r in range(1, len(its) + 1):
            for idxs in combinations(range(len(its)), r):
                # C1/C2: adjacent gap check
                valid = True
                for k in range(1, len(idxs)):
                    gap = its[idxs[k]].getTimestamp() - its[idxs[k - 1]].getTimestamp()
                    if not self._gap_ok(gap):
                        valid = False
                        break
                if not valid:
                    continue

                # C3/C4: whole interval check
                first_ts = its[idxs[0]].getTimestamp()
                last_ts = its[idxs[-1]].getTimestamp()
                if not self._whole_ok(last_ts - first_ts):
                    continue

                # All item-subset combinations with relative timestamps
                parts = []
                for i in idxs:
                    ts = its[i].getTimestamp() - first_ts
                    items = [x.getId() for x in its[i].getItems()]
                    combos = []
                    for k in range(1, len(items) + 1):
                        for c in combinations(items, k):
                            combos.append((ts, tuple(sorted(c))))
                    parts.append(combos)

                for prod in product(*parts):
                    results.add(tuple(prod))

        return results

    def _rep_to_seq(self, rep):
        s = Sequence(0)
        for ts, items in rep:
            s.addItemset(Itemset([ItemSimple(x) for x in items], ts))
        return s


# ---------------------------------------------------------------------------
# AlgoSeqDim — the main SEQ-DIM algorithm
# ---------------------------------------------------------------------------

class AlgoSeqDim:
    """Equivalent to AlgoSeqDim.java"""

    def __init__(self):
        self.patternCount = 0
        self.startTime = 0
        self.endTime = 0

    def runAlgorithm(self, db: MDSequenceDatabase, minsup: float,
                     min_interval=0, max_interval=float('inf'),
                     min_whole_interval=0, max_whole_interval=float('inf'),
                     outpath="output.txt"):
        """
        Run the SEQ-DIM algorithm for closed multi-dimensional sequential pattern mining.

        Parameters
        ----------
        db                  : MDSequenceDatabase
        minsup              : minimum support in (0, 1]
        min_interval        : C1 — min time gap between adjacent itemsets
        max_interval        : C2 — max time gap between adjacent itemsets
        min_whole_interval  : C3 — min total time span of a sequence pattern
        max_whole_interval  : C4 — max total time span of a sequence pattern
        outpath             : path of the output file
        """
        # Mirror Java constructor validation
        if min_interval > max_interval:
            raise ValueError(
                f"min_interval ({min_interval}) > max_interval ({max_interval})")
        if min_whole_interval > max_whole_interval:
            raise ValueError(
                f"min_whole_interval ({min_whole_interval}) > "
                f"max_whole_interval ({max_whole_interval})")
        if min_interval > max_whole_interval:
            raise ValueError(
                f"min_interval ({min_interval}) > "
                f"max_whole_interval ({max_whole_interval})")
        if max_interval > max_whole_interval:
            raise ValueError(
                f"max_interval ({max_interval}) > "
                f"max_whole_interval ({max_whole_interval})")

        MemoryLogger.getInstance().reset()
        self.patternCount = 0
        self.startTime = int(time.time() * 1000)

        # Step 1: Mine frequent closed sequential patterns (with time constraints)
        algoSeq = AlgoSeq(
            min_interval=min_interval,
            max_interval=max_interval,
            min_whole_interval=min_whole_interval,
            max_whole_interval=max_whole_interval,
        )
        sequences = algoSeq.runAlgorithm(db.seqdb, minsup)

        # Step 2: For each closed sequence, mine closed MD patterns over the
        # projected sub-database, then combine into MD-sequences.
        algoDim = AlgoDim()
        all_patterns = []

        for sequence in sequences:
            proj_sids_sorted = sorted(sequence._sids)
            projected = [db.mdpats[sid] for sid in proj_sids_sorted]
            if not projected:
                continue

            # Adjust minsup for the projected sub-database (Java semantics)
            newMin = minsup * db.size() / len(projected)
            mdpatterns = algoDim.runAlgorithm(projected, newMin)

            for mdp in mdpatterns:
                mds = MDSequence(mdp, sequence)
                if mdp.isAllWildcards():
                    # All-wildcard support = support of the sequence
                    mds.setSupport(sequence.getAbsoluteSupport())
                else:
                    # Map local AlgoDim indices back to global sequence IDs
                    global_sids = {proj_sids_sorted[li] for li in mdp.sids}
                    mds.setSupport(len(global_sids))
                all_patterns.append(mds)

        # Step 3: Remove non-closed MD-sequences (mirrors Java removeRedundancy()).
        #
        # Java's removeRedundancy() iterates levels from highest to 1 (i > 0),
        # where level = sequence length (number of itemsets).  Level 0 would be
        # the all-wildcard MD pattern, which is therefore NEVER written to file.
        #
        # For each non-wildcard pattern p, it is output only if no other
        # non-wildcard pattern q with the same support, seq.size() >= p.seq.size(),
        # strictly contains p.
        final = []
        for p in all_patterns:
            if p.md.isAllWildcards():   # Java level 0 — never output
                continue
            is_closed = True
            for q in all_patterns:
                if p is q:
                    continue
                if q.md.isAllWildcards():   # wildcard never used as dominator
                    continue
                if q.seq.size() < p.seq.size():  # dominator must be at same/higher level
                    continue
                if q.getAbsoluteSupport() != p.getAbsoluteSupport():
                    continue
                if q.contains(p):
                    is_closed = False
                    break
            if is_closed:
                final.append(p)

        # Output — sort to match Java output order
        final.sort(key=lambda x: (-x.seq.size(),
                                  x.seq.toStringShort(),
                                  x.md.toStringShort()))

        if final:
            print("\n --- FREQUENT CLOSED MD-SEQUENCES FOUND ---")
        with open(outpath, "w", encoding="utf-8") as f:
            for r in final:
                line = (r.md.toStringShort()
                        + r.seq.toStringShort()
                        + " #SUP: " + str(r.getAbsoluteSupport()))
                f.write(line + "\n")
                print(f" [Pattern {self.patternCount + 1}] {line}")
                self.patternCount += 1
        if final:
            print(" ----------------------------------------")

        self.endTime = int(time.time() * 1000)
        MemoryLogger.getInstance().checkMemory()

    def printStatistics(self, dbsize):
        print("=============  SEQ-DIM - STATISTICS =============")
        print(f" Total time ~ {self.endTime - self.startTime} ms")
        print(f" max memory : {MemoryLogger.getInstance().getMaxMemory()}")
        print(f" Frequent closed MD-sequences count : {self.patternCount}")
        print("=================================================")


# ---------------------------------------------------------------------------
# Optional Java comparison helpers
# ---------------------------------------------------------------------------

def _normalize(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return [" ".join(ln.split()) for ln in f if ln.strip()]


def compare_outputs(py_path: str, java_path: str):
    py = sorted(_normalize(py_path))
    jv = sorted(_normalize(java_path))
    if py == jv:
        print("MATCH: Python output is 100% identical to Java output.")
        return True
    print("MISMATCH: Python output differs from Java output.")
    print(f"  Python lines : {len(py)},  Java lines : {len(jv)}")
    py_only = sorted(set(py) - set(jv))
    jv_only = sorted(set(jv) - set(py))
    for ln in py_only[:5]:
        print("  PYTHON only:", ln)
    for ln in jv_only[:5]:
        print("  JAVA   only:", ln)
    return False

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("=" * 62)
    print("SEQ-DIM: Closed Multi-Dimensional Sequential Pattern Mining")
    print("=" * 62)
    print()


    minsup    = MIN_SUPPORT
    min_int   = MIN_INTERVAL
    max_int   = MAX_INTERVAL
    min_whole = MIN_WHOLE_INTERVAL
    max_whole = MAX_WHOLE_INTERVAL

    # Validate
    if not (0 < minsup <= 1):
        print("Error: MIN_SUPPORT must be in (0, 1].")
        return
    if min_int > max_int:
        print(f"Error: MIN_INTERVAL ({min_int}) > MAX_INTERVAL ({max_int}).")
        return
    if min_whole > max_whole:
        print(f"Error: MIN_WHOLE_INTERVAL ({min_whole}) > MAX_WHOLE_INTERVAL ({max_whole}).")
        return
    if min_int > max_whole:
        print(f"Error: MIN_INTERVAL ({min_int}) > MAX_WHOLE_INTERVAL ({max_whole}).")
        return
    if max_int > max_whole:
        print(f"Error: MAX_INTERVAL ({max_int}) > MAX_WHOLE_INTERVAL ({max_whole}).")
        return

    # Load
    db = MDSequenceDatabase()
    print(f"Loading : {INPUT_FILE}")
    db.loadFile(INPUT_FILE)
    print(f"Database size : {db.size()} sequences")
    print()
    print("Parameters:")
    print(f"  minsup                   = {minsup} ({minsup * 100:.0f}%)")
    print(f"  minInterval         (C1) = {min_int}")
    print(f"  maxInterval         (C2) = {max_int}")
    print(f"  minWholeInterval    (C3) = {min_whole}")
    print(f"  maxWholeInterval    (C4) = {max_whole}")
    print()

    # Run
    algo = AlgoSeqDim()
    print("Running SEQ-DIM algorithm...")
    algo.runAlgorithm(
        db, minsup,
        min_interval=min_int,
        max_interval=max_int,
        min_whole_interval=min_whole,
        max_whole_interval=max_whole,
        outpath=OUTPUT_FILE,
    )
    print()
    algo.printStatistics(db.size())
    print(f"Output written to: {OUTPUT_FILE}")
    print()

if __name__ == "__main__":
    main()