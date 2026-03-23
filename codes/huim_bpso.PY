"""
HUIM-BPSO: High Utility Itemset Mining using Binary Particle Swarm Optimization
Python conversion of the Java implementation by Jerry Chun-Wei Lin, Lu Yang, Philippe Fournier-Viger (2016)

Usage:
    python huim_bpso.py                          # uses default settings
    python huim_bpso.py --input data.txt --min_utility 40 --output output.txt
    python huim_bpso.py --compare                # run and compare with reference output
"""

import sys
import os
import math
import random
import time
import argparse
from collections import defaultdict
from copy import deepcopy


# ─────────────────────────────────────────────
#  ALGORITHM CONSTANTS  (mirror Java constants)
# ─────────────────────────────────────────────
POP_SIZE   = 20
ITERATIONS = 10000
C1         = 2
C2         = 2
W          = 0.9


# ─────────────────────────────────────────────
#  DATA CLASSES
# ─────────────────────────────────────────────
class Pair:
    """An item and its utility in a transaction."""
    def __init__(self, item=0, utility=0):
        self.item    = item
        self.utility = utility


class Particle:
    """A BPSO particle (binary position vector + fitness)."""
    def __init__(self, length=0):
        self.X       = [0] * length   # binary position vector
        self.fitness = 0

    def copy_from(self, other: "Particle"):
        """Deep-copy another particle into self (mirrors Java copyParticle)."""
        for i in range(len(other.X)):
            self.X[i] = other.X[i]
        self.fitness = other.fitness


class HUI:
    """A discovered High Utility Itemset."""
    def __init__(self, itemset: str, fitness: int):
        self.itemset = itemset
        self.fitness = fitness


# ─────────────────────────────────────────────
#  ALGORITHM CLASS
# ─────────────────────────────────────────────
class AlgoHUIM_BPSO:

    def __init__(self):
        self.max_memory     = 0.0
        self.start_time     = 0
        self.end_time       = 0

        self.map_item_to_twu  = {}   # TWU of every item seen
        self.map_item_to_twu0 = {}   # TWU after pruning items below minUtil

        self.twu_pattern  = []       # sorted list of promising items
        self.database     = []       # list of transactions; each = list of Pair
        self.hui_sets     = []       # discovered HUIs
        self.percentage   = []       # roulette-wheel cumulative percentages

        self.population   = []       # current swarm
        self.p_best       = []       # personal best per particle
        self.g_best       = None     # global best particle
        self.V            = []       # velocities  [pop_size][twuPattern.size]

    # ── public entry point ──────────────────────────────────────────────────
    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.max_memory = 0.0
        self.start_time = time.time()

        # Reset mutable state so repeated calls on the same object are deterministic.
        self.twu_pattern = []
        self.database = []
        self.hui_sets = []
        self.percentage = []
        self.population = []
        self.p_best = []
        self.g_best = None
        self.V = []

        # ── PASS 1: compute TWU for every item ──────────────────────────────
        self.map_item_to_twu  = {}
        self.map_item_to_twu0 = {}

        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts              = line.split(":")
                items              = parts[0].strip().split()
                transaction_utility = int(parts[1].strip())

                for item_str in items:
                    item = int(item_str)
                    self.map_item_to_twu[item]  = self.map_item_to_twu.get(item,  0) + transaction_utility
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + transaction_utility

        # ── PASS 2: build database, keep only items with TWU >= minUtility ──
        self.database = []

        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts          = line.split(":")
                items          = parts[0].strip().split()
                utility_values = parts[2].strip().split()

                revised_transaction = []
                for i, item_str in enumerate(items):
                    pair         = Pair()
                    pair.item    = int(item_str)
                    pair.utility = int(utility_values[i])

                    if self.map_item_to_twu.get(pair.item, 0) >= min_utility:
                        revised_transaction.append(pair)
                    else:
                        # remove item from twu0 map (mirrors Java logic)
                        self.map_item_to_twu0.pop(pair.item, None)

                self.database.append(revised_transaction)

        # twuPattern = sorted promising items
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        # initialise pBest list and gBest
        self.p_best = [Particle(len(self.twu_pattern)) for _ in range(POP_SIZE)]
        self.g_best  = Particle(len(self.twu_pattern))

        self._check_memory()

        if len(self.twu_pattern) > 0:
            self._generate_pop(min_utility)
            for _ in range(ITERATIONS):
                self._update(min_utility)

        self._write_out(output_path)
        self._check_memory()
        self.end_time = time.time()

    # ── population initialisation ────────────────────────────────────────────
    def _generate_pop(self, min_utility: int):
        self.percentage = self._roulette_percent()

        for i in range(POP_SIZE):
            temp_particle = Particle(len(self.twu_pattern))
            j = 0
            # k = random number of 1-bits in this particle
            k = int(random.random() * len(self.twu_pattern))

            while j < k:
                temp = self._roulette_select(self.percentage)
                if temp_particle.X[temp] == 0:
                    j += 1
                    temp_particle.X[temp] = 1

            temp_particle.fitness = self._fit_calculate(temp_particle.X, k)
            self.population.append(temp_particle)

            # initialise pBest
            self.p_best[i].copy_from(temp_particle)

            # update huiSets
            if self.population[i].fitness >= min_utility:
                self._insert(self.population[i])

            # initialise gBest
            if i == 0:
                self.g_best.copy_from(self.p_best[i])
            else:
                if self.p_best[i].fitness > self.g_best.fitness:
                    self.g_best.copy_from(self.p_best[i])

            # initialise velocity with random values
            self.V.append([random.random() for _ in range(len(self.twu_pattern))])

    # ── swarm update (one iteration) ─────────────────────────────────────────
    def _update(self, min_utility: int):
        for i in range(POP_SIZE):
            k   = 0
            r1  = random.random()
            r2  = random.random()

            # update velocity
            for j in range(len(self.twu_pattern)):
                new_v = (self.V[i][j]
                         + r1 * (self.p_best[i].X[j] - self.population[i].X[j])
                         + r2 * (self.g_best.X[j]    - self.population[i].X[j]))
                # clamp to [-2, 2]
                new_v        = max(-2.0, min(2.0, new_v))
                self.V[i][j] = new_v

            # update particle position (sigmoid transfer)
            for j in range(len(self.twu_pattern)):
                temp1 = random.random()
                temp2 = 1.0 / (1.0 + math.exp(-self.V[i][j]))
                if temp1 < temp2:
                    self.population[i].X[j] = 1
                    k += 1
                else:
                    self.population[i].X[j] = 0

            # calculate fitness
            self.population[i].fitness = self._fit_calculate(self.population[i].X, k)

            # update pBest and gBest
            if self.population[i].fitness > self.p_best[i].fitness:
                self.p_best[i].copy_from(self.population[i])
                if self.p_best[i].fitness > self.g_best.fitness:
                    self.g_best.copy_from(self.p_best[i])

            # update huiSets
            if self.population[i].fitness >= min_utility:
                self._insert(self.population[i])

    # ── insert a particle into huiSets if not already present ────────────────
    def _insert(self, temp_particle: Particle):
        parts = []
        for i in range(len(self.twu_pattern)):
            if temp_particle.X[i] == 1:
                parts.append(str(self.twu_pattern[i]))
        itemset_str = " ".join(parts) + " "   # trailing space mirrors Java

        if not self.hui_sets:
            self.hui_sets.append(HUI(itemset_str, temp_particle.fitness))
        else:
            for hui in self.hui_sets:
                if hui.itemset == itemset_str:
                    return                     # already exists
            self.hui_sets.append(HUI(itemset_str, temp_particle.fitness))

    # ── roulette-wheel initialisation ────────────────────────────────────────
    def _roulette_percent(self):
        total    = sum(self.map_item_to_twu[item] for item in self.twu_pattern)
        temp_sum = 0
        pcts     = []
        for item in self.twu_pattern:
            temp_sum += self.map_item_to_twu[item]
            pcts.append(temp_sum / (total + 0.0))
        return pcts

    # ── roulette-wheel selection ─────────────────────────────────────────────
    def _roulette_select(self, percentage):
        rand_num = random.random()
        for i, pct in enumerate(percentage):
            if i == 0:
                if 0 <= rand_num <= pct:
                    return 0
            elif percentage[i - 1] < rand_num <= pct:
                return i
        return len(percentage) - 1   # safety fallback

    # ── fitness calculation ───────────────────────────────────────────────────
    def _fit_calculate(self, temp_particle, k: int) -> int:
        """
        Mirrors Java fitCalculate exactly.
        Sums utilities of the itemset encoded in temp_particle across all transactions.
        The itemset must appear completely in a transaction to count.
        """
        if k == 0:
            return 0

        fitness = 0
        for transaction in self.database:
            i = j = q = temp = s = 0
            while j < k and q < len(transaction) and i < len(temp_particle):
                if temp_particle[i] == 1:
                    if transaction[q].item < self.twu_pattern[i]:
                        q += 1
                    elif transaction[q].item == self.twu_pattern[i]:
                        s    += transaction[q].utility
                        j    += 1
                        q    += 1
                        temp += 1
                        i    += 1
                    else:   # transaction[q].item > twuPattern[i]
                        j += 1
                        i += 1
                else:
                    i += 1
            if temp == k:
                fitness += s
        return fitness

    # ── write output ──────────────────────────────────────────────────────────
    def _write_out(self, output_path: str):
        with open(output_path, "w", newline="") as f:
            for hui in self.hui_sets:
                f.write(f"{hui.itemset}#UTIL: {hui.fitness}\n")
            f.write("\n")

    # ── memory helper ─────────────────────────────────────────────────────────
    def _check_memory(self):
        # Python has no direct equivalent; use a rough estimate via sys
        import tracemalloc
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / 1024 / 1024
        if current_mb > self.max_memory:
            self.max_memory = current_mb

    # ── stats ─────────────────────────────────────────────────────────────────
    def print_stats(self):
        elapsed_ms = (self.end_time - self.start_time) * 1000
        print("=============  HUIM-BPSO ALGORITHM v.2.36 - STATS =============")
        print(f" Total time ~ {elapsed_ms:.0f} ms")
        print(f" Memory ~ {self.max_memory:.4f} MB")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")


# ─────────────────────────────────────────────
#  COMPARISON HELPER
# ─────────────────────────────────────────────
def compare_outputs(java_output_path: str, python_output_path: str):
    """
    Compare Java and Python output files.
    Both files are expected in format:  <items> #UTIL: <value>
    Comparison is SET-based (order-independent).
    """
    def parse_output(path):
        results = {}
        if not os.path.exists(path):
            return results
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or "#UTIL:" not in line:
                    continue
                itemset_part, util_part = line.split("#UTIL:")
                itemset = frozenset(int(x) for x in itemset_part.strip().split())
                util    = int(util_part.strip())
                results[itemset] = util
        return results

    java_results   = parse_output(java_output_path)
    python_results = parse_output(python_output_path)

    print("\n========== OUTPUT COMPARISON ==========")
    print(f"  Java   output : {len(java_results)} itemsets")
    print(f"  Python output : {len(python_results)} itemsets")

    only_in_java   = set(java_results)   - set(python_results)
    only_in_python = set(python_results) - set(java_results)
    in_both        = set(java_results)   & set(python_results)

    util_mismatches = []
    for itemset in in_both:
        if java_results[itemset] != python_results[itemset]:
            util_mismatches.append((itemset, java_results[itemset], python_results[itemset]))

    if not only_in_java and not only_in_python and not util_mismatches:
        print("  ✅  MATCH — outputs are identical!")
    else:
        print("  ❌  MISMATCH detected:")
        if only_in_java:
            print(f"    In Java only   ({len(only_in_java)}):")
            for s in sorted(only_in_java, key=lambda x: sorted(x)):
                print(f"      {sorted(s)}  #UTIL: {java_results[s]}")
        if only_in_python:
            print(f"    In Python only ({len(only_in_python)}):")
            for s in sorted(only_in_python, key=lambda x: sorted(x)):
                print(f"      {sorted(s)}  #UTIL: {python_results[s]}")
        if util_mismatches:
            print(f"    Utility mismatch ({len(util_mismatches)}):")
            for itemset, ju, pu in util_mismatches:
                print(f"      {sorted(itemset)}  Java={ju}  Python={pu}")
    print("=======================================\n")

    return len(only_in_java) == 0 and len(only_in_python) == 0 and len(util_mismatches) == 0


def resolve_default_input_path(script_dir: str) -> str:
    """Find the bundled dataset across common project layouts."""
    candidates = [
        os.path.join(script_dir, "contextHUIM.txt"),
        os.path.join(script_dir, "..", "Java", "src", "contextHUIM.txt"),
        os.path.join(script_dir, "..", "Java", "bin", "contextHUIM.txt"),
        os.path.join(script_dir, "..", "contextHUIM.txt"),
    ]

    for candidate in candidates:
        path = os.path.abspath(candidate)
        if os.path.exists(path):
            return path

    # Keep old relative default behavior as a final fallback.
    return "contextHUIM.txt"


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(description="HUIM-BPSO High Utility Itemset Mining")
    parser.add_argument("--input",       default=None,  help="Input transaction file")
    parser.add_argument("--output",      default=None, help="Python output file")
    parser.add_argument("--min_utility", type=int, default=25,        help="Minimum utility threshold")
    parser.add_argument("--compare",     action="store_true",         help="Compare with a Java output file")
    parser.add_argument("--java_output", default=None,   help="Java output file for comparison")
    args = parser.parse_args()

    input_path = os.path.abspath(args.input) if args.input else resolve_default_input_path(script_dir)
    output_path = os.path.abspath(args.output) if args.output else os.path.join(script_dir, "python_output.txt")
    java_output_path = os.path.abspath(args.java_output) if args.java_output else os.path.join(script_dir, "..", "Java", "output.txt")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        print("Use --input <path> or place contextHUIM.txt in Python/, Java/src/, Java/bin/, or project root.")
        sys.exit(1)

    print(f"Running HUIM-BPSO  |  input={input_path}  |  min_utility={args.min_utility}")

    algo = AlgoHUIM_BPSO()
    algo.run_algorithm(input_path, output_path, args.min_utility)
    algo.print_stats()

    print(f"\nOutput written to: {output_path}")

    # Optional comparison
    if args.compare:
        if not os.path.exists(java_output_path):
            print(f"[compare] Java output file not found: {java_output_path}")
            print("          Run the Java version first and save its output, then re-run with --compare")
        else:
            compare_outputs(java_output_path, output_path)


if __name__ == "__main__":
    main()