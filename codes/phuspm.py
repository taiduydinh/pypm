"""
PHUSPM - High Utility-Probability Sequential Pattern Mining
Python conversion of the Java implementation by Ting Li et al., 2018

Reference:
    Zhang, B., Lin, J. C.-W., Li, T., Gan, W., Fournier-Viger, P. (2017).
    Mining High Utility-Probability Sequential Patterns in Uncertain Databases.
    PLoS One, Public Library of Science.

Usage:
    python phuspm.py
    - Edit the parameters at the bottom of this file to change input/output paths,
      minUtility, and minProbability.
"""

import time
import os
import sys
from copy import deepcopy


# ─────────────────────────────────────────────
#  Data classes  (mirror of Java classes)
# ─────────────────────────────────────────────

class Item:
    """Mirrors Item.java"""
    def __init__(self, item: int, utility: int):
        self.item = item
        self.utility = utility


class Itemset:
    """Mirrors Itemset.java"""
    def __init__(self):
        self.items: list[Item] = []   # named 'Itemset' in Java, 'items' here for clarity


class Element:
    """Mirrors Element.java"""
    def __init__(self, SID: int, location: int, utility: int,
                 probability: float, rest_utility: int):
        self.SID = SID
        self.location = location
        self.utility = utility
        self.probability = probability
        self.rest_utility = rest_utility   # restUtility in Java


class SequenceList:
    """Mirrors SequenceList.java"""
    def __init__(self):
        self.itemsets: list[list[int]] = []   # list of itemsets, each itemset is list of ints
        self.elements: list[Element] = []
        self.sum_utility: int = 0
        self.sum_probability: float = 0.0
        self.sum_swu: int = 0             # sumSWU in Java

    def add_element(self, element: Element):
        self.elements.append(element)

    def add_itemset(self, itemset: list[int]):
        self.itemsets.append(itemset)

    def calculate(self):
        """
        Recalculate sumUtility, sumProbability, sumSWU from elements.
        Mirrors SequenceList.calculate() in Java exactly.
        """
        if not self.elements:
            return

        order = self.elements[0].SID
        order_utility = self.elements[0].utility
        order_probability = self.elements[0].probability
        order_suub = self.elements[0].rest_utility + self.elements[0].utility

        for element in self.elements:
            if element.SID == order:
                if element.utility > order_utility:
                    order_utility = element.utility
                if element.probability > order_probability:
                    order_probability = element.probability
                if element.rest_utility + element.utility > order_suub:
                    order_suub = element.rest_utility + element.utility
            else:
                self.sum_utility += order_utility
                self.sum_probability += order_probability
                self.sum_swu += order_suub

                order = element.SID
                order_utility = element.utility
                order_probability = element.probability
                order_suub = element.rest_utility + element.utility

        # flush last group
        self.sum_utility += order_utility
        self.sum_probability += order_probability
        self.sum_swu += order_suub


# ─────────────────────────────────────────────
#  Memory logger  (mirrors MemoryLogger.java)
# ─────────────────────────────────────────────

class MemoryLogger:
    _instance = None

    def __init__(self):
        self.max_memory: float = 0.0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def get_max_memory(self) -> float:
        return self.max_memory

    def reset(self):
        self.max_memory = 0.0

    def check_memory(self) -> float:
        try:
            import resource
            # getrusage returns KB on Linux
            current = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
        except Exception:
            current = 0.0
        if current > self.max_memory:
            self.max_memory = current
        return current


# ─────────────────────────────────────────────
#  Main algorithm  (mirrors AlgoPHUSPM.java)
# ─────────────────────────────────────────────

class AlgoPHUSPM:
    def __init__(self):
        self.max_memory: int = 0
        self.start_timestamp: float = 0
        self.end_timestamp: float = 0
        self.number_of_husp: int = 0
        self.number_of_candidates: int = 0
        self._output_lines: list[str] = []   # collects output lines (replaces BufferedWriter)

    # ── public entry point ──────────────────────────────────────────────

    def run_algorithm(self, input_path: str, output_path: str,
                      min_utility: int, min_probability: float):
        """
        Mirrors AlgoPHUSPM.runAlgorithm().
        """
        self.max_memory = 0
        self.start_timestamp = time.time()
        self.number_of_husp = 0
        self.number_of_candidates = 0
        self._output_lines = []

        # ── Pass 1: compute RSU and Probability for every individual item ──
        RSU: dict[int, int] = {}          # Remaining-Sequence Utility per item
        Probability: dict[int, float] = {}

        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(' -1 ')
                line_utility = int(parts[-2])
                line_probability = float(parts[-1])

                seen_items: set[int] = set()
                for i in range(len(parts) - 2):
                    for item_str in parts[i].split(' , '):
                        tokens = item_str.split()
                        seen_items.add(int(tokens[0]))

                for item in seen_items:
                    RSU[item] = RSU.get(item, 0) + line_utility
                    Probability[item] = Probability.get(item, 0.0) + line_probability

        # ── Pass 2: build revised database ──────────────────────────────
        sequence_list_map: dict[int, SequenceList] = {}
        order_swu: dict[int, int] = {}
        order_swp: dict[int, float] = {}
        revised_database: list[list[Itemset]] = []

        with open(input_path, 'r') as f:
            order = 0
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(' -1 ')
                line_swu = int(parts[-2])
                line_swp = float(parts[-1])

                sequence: list[Itemset] = []
                for i in range(len(parts) - 2):
                    sitemset = Itemset()
                    for item_str in parts[i].split(' , '):
                        tokens = item_str.split()
                        item = int(tokens[0])
                        utility = int(tokens[1])
                        if (RSU.get(item, 0) >= min_utility and
                                Probability.get(item, 0.0) >= min_probability):
                            sitemset.items.append(Item(item, utility))
                        else:
                            line_swu -= utility

                    if sitemset.items:
                        sequence.append(sitemset)

                if sequence:
                    revised_database.append(sequence)
                    order_swu[order] = line_swu
                    order_swp[order] = line_swp
                    order += 1

        # ── Build sequence-list map for 1-sequences ──────────────────────
        for i, seq in enumerate(revised_database):
            line_swu = order_swu[i]
            line_probability = order_swp[i]
            rest_utility = line_swu

            for j, itemset in enumerate(seq):
                for k, it in enumerate(itemset.items):
                    rest_utility -= it.utility
                    element = Element(i, j, it.utility, line_probability, rest_utility)

                    if it.item in sequence_list_map:
                        sequence_list_map[it.item].add_element(element)
                    else:
                        new_seq = SequenceList()
                        new_seq.add_itemset([it.item])
                        new_seq.add_element(element)
                        sequence_list_map[it.item] = new_seq

        # ── Filter and sort 1-sequences ───────────────────────────────────
        one_sequence_list: list[SequenceList] = list(sequence_list_map.values())
        filtered = []
        for sl in one_sequence_list:
            sl.calculate()
            if sl.sum_swu >= min_utility and sl.sum_probability >= min_probability:
                filtered.append(sl)
        one_sequence_list = filtered

        # Sort by first item of first itemset (mirrors Java Comparator)
        one_sequence_list.sort(key=lambda sl: sl.itemsets[0][0])

        # ── Mine recursively ──────────────────────────────────────────────
        for seq in one_sequence_list:
            self._mine(seq, revised_database, min_utility, min_probability)
        self.number_of_candidates += len(one_sequence_list)

        MemoryLogger.get_instance().check_memory()

        # Write output
        with open(output_path, 'w') as f:
            for line in self._output_lines:
                f.write(line + '\n')

        self.end_timestamp = time.time()

    # ── recursive mining ────────────────────────────────────────────────

    def _mine(self, seq: SequenceList, revised_database: list[list[Itemset]],
              min_utility: int, min_probability: float):
        """
        Mirrors AlgoPHUSPM.AlgoPHUSPM() (the recursive method).
        """
        self.number_of_candidates += 1

        if seq.sum_utility >= min_utility and seq.sum_probability >= min_probability:
            self.number_of_husp += 1
            self._write_out(seq)

        MemoryLogger.get_instance().check_memory()

        next_generation: list[SequenceList] = []
        item_extend: dict[int, SequenceList] = {}
        itemset_extend: dict[int, SequenceList] = {}

        last_item = seq.itemsets[-1][-1]   # last item of last itemset

        for element in seq.elements:
            SID = element.SID
            location = element.location
            pre_utility = 0

            # Find position of last_item in current itemset, then advance past it
            current_itemset = revised_database[SID][location].items
            i = 0
            while i < len(current_itemset):
                if current_itemset[i].item == last_item:
                    i += 1
                    break
                i += 1

            # ── item-based extension (same itemset, items after last_item) ──
            for idx in range(i, len(current_itemset)):
                it = current_itemset[idx]
                pre_utility += it.utility

                new_element = Element(
                    SID, location,
                    element.utility + it.utility,
                    element.probability,
                    element.rest_utility - pre_utility
                )

                if it.item not in item_extend:
                    new_list = SequenceList()
                    # copy existing itemsets (deep copy of lists of ints)
                    for existing_itemset in seq.itemsets:
                        new_list.itemsets.append(list(existing_itemset))
                    # append last_item's itemset extended with new item
                    new_last = list(new_list.itemsets[-1])
                    new_last.append(it.item)
                    new_list.itemsets[-1] = new_last

                    new_list.add_element(new_element)
                    item_extend[it.item] = new_list
                else:
                    item_extend[it.item].add_element(new_element)

            # ── itemset-based extension (subsequent itemsets) ──────────────
            for j in range(element.location + 1, len(revised_database[SID])):
                for it in revised_database[SID][j].items:
                    pre_utility += it.utility

                    new_element = Element(
                        SID, j,
                        element.utility + it.utility,
                        element.probability,
                        element.rest_utility - pre_utility
                    )

                    if it.item not in itemset_extend:
                        new_list = SequenceList()
                        for existing_itemset in seq.itemsets:
                            new_list.itemsets.append(list(existing_itemset))
                        new_list.itemsets.append([it.item])

                        new_list.add_element(new_element)
                        itemset_extend[it.item] = new_list
                    else:
                        itemset_extend[it.item].add_element(new_element)

        # ── Filter candidates by SWU/probability pruning ──────────────────
        for sl in item_extend.values():
            sl.calculate()
            if sl.sum_swu >= min_utility and sl.sum_probability >= min_probability:
                next_generation.append(sl)

        for sl in itemset_extend.values():
            sl.calculate()
            if sl.sum_swu >= min_utility and sl.sum_probability >= min_probability:
                next_generation.append(sl)

        for next_seq in next_generation:
            self._mine(next_seq, revised_database, min_utility, min_probability)

    # ── output ───────────────────────────────────────────────────────────

    def _write_out(self, sequence: SequenceList):
        """Mirrors writeOut() in Java."""
        parts = []
        for itemset in sequence.itemsets:
            for item in itemset:
                parts.append(str(item) + ' ')
            parts.append('-1 ')
        parts.append('#UITL: ')
        parts.append(str(sequence.sum_utility))
        parts.append(' #SP: ')
        # Java uses Float.toString() which gives format like "2.8" not "2.7999999..."
        # We round to 7 significant figures to match Java float precision
        sp_val = sequence.sum_probability
        parts.append(_float_to_java_string(sp_val))
        self._output_lines.append(''.join(parts))

    def print_stats(self):
        """Mirrors printStats() in Java."""
        print("=======  THE RESULT OF THE ALGORITHM - STATS ============")
        elapsed = self.end_timestamp - self.start_timestamp
        print(f" Total time ~ {elapsed:.3f} s")
        print(f" Candidates count: {self.number_of_candidates}")
        print(f" HUSPs count: {self.number_of_husp}")
        print(f" Max memory: {MemoryLogger.get_instance().get_max_memory():.2f} MB")
        print("======================================================")


# ─────────────────────────────────────────────
#  Float formatting helper
#  Java's Float.toString() trims trailing zeros and uses minimal digits.
#  Python's str(float) can differ slightly; this helper mimics Java.
# ─────────────────────────────────────────────

def _float_to_java_string(value: float) -> str:
    """
    Format a float the same way Java's Float.toString() does:
    - No unnecessary trailing zeros beyond what's needed
    - Always at least one decimal place (e.g. 2.0 not 2)
    """
    # Use repr for full precision, then strip trailing zeros but keep at least X.X
    s = f"{value:.7g}"
    if '.' not in s and 'e' not in s:
        s += '.0'
    return s


# ─────────────────────────────────────────────
#  Comparison / testing helper
# ─────────────────────────────────────────────

def compare_outputs(java_output_path: str, python_output_path: str) -> bool:
    """
    Compare Java and Python output files line by line (order-independent).
    Returns True if they match 100%.
    """
    def parse_output(path):
        patterns = set()
        if not os.path.exists(path):
            print(f"  [!] File not found: {path}")
            return patterns
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    patterns.add(line)
        return patterns

    java_set = parse_output(java_output_path)
    python_set = parse_output(python_output_path)

    print(f"\n{'='*55}")
    print(f"  OUTPUT COMPARISON")
    print(f"{'='*55}")
    print(f"  Java   output patterns : {len(java_set)}")
    print(f"  Python output patterns : {len(python_set)}")

    only_in_java = java_set - python_set
    only_in_python = python_set - java_set

    if only_in_java:
        print(f"\n  [MISSING in Python - {len(only_in_java)} pattern(s)]:")
        for p in sorted(only_in_java):
            print(f"    {p}")

    if only_in_python:
        print(f"\n  [EXTRA in Python - {len(only_in_python)} pattern(s)]:")
        for p in sorted(only_in_python):
            print(f"    {p}")

    if not only_in_java and not only_in_python:
        print(f"\n  ✅  MATCH: Both outputs are IDENTICAL ({len(java_set)} patterns)")
        print(f"{'='*55}\n")
        return True
    else:
        print(f"\n  ❌  MISMATCH detected!")
        print(f"{'='*55}\n")
        return False


# ─────────────────────────────────────────────
#  Entry point — edit parameters here
# ─────────────────────────────────────────────

if __name__ == '__main__':

    # ── Parameters ────────────────────────────────────────────────────────
    INPUT_FILE       = 'contextPHUSPM.txt'   # path to input data file
    OUTPUT_FILE      = 'output_python.txt'   # Python output
    MIN_UTILITY      = 20                    # integer  (same as Java: int minUtility)
    MIN_PROBABILITY  = 1.6                   # float    (same as Java: float minProbability)

    # Optional: if you have a Java output to compare against, set the path here.
    # Leave as None to skip comparison.
    JAVA_OUTPUT_FILE = None   # e.g. 'output_java.txt'
    # ─────────────────────────────────────────────────────────────────────

    # Auto-find the input file in common locations
    if not os.path.exists(INPUT_FILE):
        # Try looking in Java/src subdirectory
        alt_path = os.path.join('Java', 'src', INPUT_FILE)
        if os.path.exists(alt_path):
            INPUT_FILE = alt_path
        else:
            print(f"[ERROR] Input file not found: {INPUT_FILE}")
            print(f"[ERROR] Also checked: {alt_path}")
            sys.exit(1)

    print(f"Running PHUSPM on '{INPUT_FILE}'")
    print(f"  minUtility    = {MIN_UTILITY}")
    print(f"  minProbability = {MIN_PROBABILITY}")
    print()

    algo = AlgoPHUSPM()
    algo.run_algorithm(INPUT_FILE, OUTPUT_FILE, MIN_UTILITY, MIN_PROBABILITY)
    algo.print_stats()

    print(f"\nOutput written to: {OUTPUT_FILE}")

    # Show discovered patterns
    print("\n── Discovered HUSPs ──")
    with open(OUTPUT_FILE, 'r') as f:
        for line in f:
            print(" ", line.rstrip())

    # Compare with Java output if provided
    if JAVA_OUTPUT_FILE:
        compare_outputs(JAVA_OUTPUT_FILE, OUTPUT_FILE)
    else:
        print("\n[TIP] To compare with Java output, set JAVA_OUTPUT_FILE at the bottom of this script.")