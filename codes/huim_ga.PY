"""
HUIM-GA: High Utility Itemset Mining using Genetic Algorithm
Python conversion of the Java implementation by Jerry Chun-Wei Lin, Lu Yang, Philippe Fournier-Viger (2016)

Usage:
    python huim_ga.py                          # uses default settings
    python huim_ga.py --input contextHUIM.txt --min_utility 40
    python huim_ga.py --min_utility 60
"""

import random
import time
import os
import sys
import argparse
from dataclasses import dataclass, field
from typing import List, Dict, Optional


# ─────────────────────────────────────────────
#  Data structures (mirrors Java inner classes)
# ─────────────────────────────────────────────

@dataclass
class Pair:
    item: int = 0
    utility: int = 0


@dataclass
class ChroNode:
    chromosome: List[int] = field(default_factory=list)
    fitness: int = 0
    rfitness: float = 0.0
    rank: int = 0

    def __init__(self, length: int = 0):
        self.chromosome = [0] * length
        self.fitness = 0
        self.rfitness = 0.0
        self.rank = 0


@dataclass
class HUI:
    itemset: str
    fitness: int


# ─────────────────────────────────────────────
#  Algorithm class
# ─────────────────────────────────────────────

class AlgoHUIM_GA:
    POP_SIZE = 20
    ITERATIONS = 10000

    def __init__(self):
        self.max_memory: float = 0.0
        self.start_timestamp: float = 0.0
        self.end_timestamp: float = 0.0

        self.map_item_to_twu: Dict[int, int] = {}
        self.map_item_to_twu0: Dict[int, int] = {}
        self.twu_pattern: List[int] = []

        self.population: List[ChroNode] = []
        self.sub_population: List[ChroNode] = []
        self.hui_sets: List[HUI] = []
        self.database: List[List[Pair]] = []
        self.percentage: List[float] = []

        self.output_path: Optional[str] = None

    # ── public entry point ─────────────────────

    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.max_memory = 0.0
        self.start_timestamp = time.time()
        self.output_path = output_path

        # Reset state (important if reused)
        self.map_item_to_twu = {}
        self.map_item_to_twu0 = {}
        self.twu_pattern = []
        self.population = []
        self.sub_population = []
        self.hui_sets = []
        self.database = []
        self.percentage = []

        # ── Pass 1: compute TWU for every item ──
        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(':')
                items = parts[0].split()
                transaction_utility = int(parts[1])
                for item_str in items:
                    item = int(item_str)
                    self.map_item_to_twu[item] = self.map_item_to_twu.get(item, 0) + transaction_utility
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + transaction_utility

        # ── Pass 2: build database, keep only promising items ──
        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(':')
                items = parts[0].split()
                utility_values = parts[2].split()

                revised_transaction: List[Pair] = []
                for i, item_str in enumerate(items):
                    p = Pair()
                    p.item = int(item_str)
                    p.utility = int(utility_values[i])
                    if self.map_item_to_twu.get(p.item, 0) >= min_utility:
                        revised_transaction.append(p)
                    else:
                        self.map_item_to_twu0.pop(p.item, None)
                self.database.append(revised_transaction)

        # Items with TWU >= minUtility, sorted ascending
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        self._check_memory()

        # ── Main GA loop ──
        if len(self.twu_pattern) > 0:
            m = len(self.database)
            n = len(self.twu_pattern)

            self._generate_pop(min_utility)

            if m > n:
                p_min = 1.0 / m
                p_max = 1.0 / n
            else:
                p_min = 1.0 / n
                p_max = 1.0 / m

            for i in range(self.ITERATIONS):
                self._calculate_rfitness()
                while len(self.sub_population) < self.POP_SIZE:
                    temp1 = self._select_chromosome()
                    temp2 = self._select_chromosome()
                    while temp1 == temp2:
                        temp2 = self._select_chromosome()
                    self._crossover(temp1, temp2, min_utility)

                self.sub_population = self._ranked_mutation(p_max, p_min, i, min_utility)

                self.sub_population.extend(self.population)
                self._rank_data(self.sub_population)

                for j in range(len(self.population)):
                    self.population[j] = self.sub_population[j]
                self.sub_population.clear()

        self._write_out()
        self._check_memory()
        self.end_timestamp = time.time()

    # ── Population initialisation ──────────────

    def _generate_pop(self, min_utility: int):
        self.percentage = self._roulette_percent()
        i = 0
        while i < self.POP_SIZE:
            node = ChroNode(len(self.twu_pattern))
            k = int(random.random() * len(self.twu_pattern))
            j = 0
            while j < k:
                temp = self._select(self.percentage)
                if node.chromosome[temp] == 0:
                    node.chromosome[temp] = 1
                    j += 1
            node.fitness = self._fit_calculate(node.chromosome, k)
            node.rank = 0
            self.population.append(node)
            if node.fitness >= min_utility:
                self._insert(node)
            i += 1

    # ── Roulette wheel based on TWU ───────────

    def _roulette_percent(self) -> List[float]:
        total = sum(self.map_item_to_twu[item] for item in self.twu_pattern)
        pct: List[float] = []
        temp_sum = 0
        for item in self.twu_pattern:
            temp_sum += self.map_item_to_twu[item]
            pct.append(temp_sum / total)
        return pct

    def _select(self, percentage: List[float]) -> int:
        rand_num = random.random()
        temp = 0
        for i, p in enumerate(percentage):
            if i == 0:
                if 0 <= rand_num <= p:
                    temp = 0
                    break
            elif percentage[i - 1] < rand_num <= p:
                temp = i
                break
        return temp

    # ── Fitness calculation ───────────────────

    def _fit_calculate(self, chromosome: List[int], k: int) -> int:
        if k == 0:
            return 0
        fitness = 0
        for transaction in self.database:
            i = j = q = temp = sum_ = 0
            while j < k and q < len(transaction) and i < len(chromosome):
                if chromosome[i] == 1:
                    if transaction[q].item < self.twu_pattern[i]:
                        q += 1
                    elif transaction[q].item == self.twu_pattern[i]:
                        sum_ += transaction[q].utility
                        j += 1
                        q += 1
                        temp += 1
                        i += 1
                    else:  # transaction[q].item > twu_pattern[i]
                        j += 1
                        i += 1
                else:
                    i += 1
            if temp == k:
                fitness += sum_
        return fitness

    # ── rfitness (roulette fitness selection) ─

    def _calculate_rfitness(self):
        total = sum(node.fitness for node in self.population)
        temp = 0
        for node in self.population:
            temp += node.fitness
            if total == 0:
                node.rfitness = float('nan')
            else:
                node.rfitness = temp / (total + 0.0)

    def _select_chromosome(self) -> int:
        rand_num = random.random()
        temp = 0
        for i, node in enumerate(self.population):
            if i == 0:
                if 0 <= rand_num <= node.rfitness:
                    temp = 0
                    break
            elif self.population[i - 1].rfitness < rand_num <= node.rfitness:
                temp = i
                break
        return temp

    # ── Crossover ────────────────────────────

    def _crossover(self, temp1: int, temp2: int, min_utility: int):
        n = len(self.twu_pattern)
        position = int(random.random() * n)

        temp1_chro: List[int] = []
        temp2_chro: List[int] = []
        count_a = count_b = 0

        for i in range(n):
            if i <= position:
                v1 = self.population[temp2].chromosome[i]
                v2 = self.population[temp1].chromosome[i]
            else:
                v1 = self.population[temp1].chromosome[i]
                v2 = self.population[temp2].chromosome[i]
            temp1_chro.append(v1)
            if v1 == 1:
                count_a += 1
            temp2_chro.append(v2)
            if v2 == 1:
                count_b += 1

        # Keep Java behavior exactly: reuse the same node object for both insertions.
        # This mirrors Java reference semantics in the original crossover code.
        temp_node = ChroNode()
        temp_node.chromosome = temp1_chro
        temp_node.fitness = self._fit_calculate(temp1_chro, count_a)
        temp_node.rank = 0
        self.sub_population.append(temp_node)
        if temp_node.fitness >= min_utility:
            self._insert(temp_node)

        temp_node.chromosome = temp2_chro
        temp_node.fitness = self._fit_calculate(temp2_chro, count_b)
        temp_node.rank = 0
        self.sub_population.append(temp_node)
        if temp_node.fitness >= min_utility:
            self._insert(temp_node)

    # ── Ranked mutation ───────────────────────

    def _get_rank(self) -> List[int]:
        ranks: List[int] = []
        for i in range(len(self.sub_population)):
            better_count = sum(
                1 for j in range(len(self.sub_population))
                if i != j and self.sub_population[i].fitness <= self.sub_population[j].fitness
            )
            ranks.append(better_count + 1)
        return ranks

    def _ranked_mutation(self, p_max: float, p_min: float,
                         current_iteration: int, min_utility: int) -> List[ChroNode]:
        record = self._get_rank()
        for i in range(self.POP_SIZE):
            pm = ((p_max - (p_max - p_min) * current_iteration / self.ITERATIONS)
                  * record[i] / len(self.sub_population))
            if random.random() < pm:
                temp = int(random.random() * len(self.twu_pattern))
                chro = self.sub_population[i].chromosome
                chro[temp] = 1 - chro[temp]   # flip bit
                k = sum(chro)
                self.sub_population[i].fitness = self._fit_calculate(chro, k)
                if self.sub_population[i].fitness >= min_utility:
                    self._insert(self.sub_population[i])
        return self.sub_population

    # ── Rank data (selection sort descending) ─

    def _rank_data(self, pop: List[ChroNode]):
        n = len(pop)
        for i in range(n - 1):
            p = i
            for j in range(i + 1, n):
                if pop[p].fitness < pop[j].fitness:
                    p = j
            if i != p:
                # Mirror Java exactly: swap fitness values, then swap each gene in-place.
                # This preserves chromosome list identity and Java-like reference behavior.
                pop[i].fitness, pop[p].fitness = pop[p].fitness, pop[i].fitness
                for q in range(len(self.twu_pattern)):
                    temp = pop[i].chromosome[q]
                    pop[i].chromosome[q] = pop[p].chromosome[q]
                    pop[p].chromosome[q] = temp
            pop[i].rank = i + 1
        pop[n - 1].rank = n

    # ── Insert into HUI set ───────────────────

    def _insert(self, node: ChroNode):
        label = ' '.join(
            str(self.twu_pattern[i])
            for i in range(len(self.twu_pattern))
            if node.chromosome[i] == 1
        ) + ' '
        if not self.hui_sets:
            self.hui_sets.append(HUI(label, node.fitness))
        else:
            for hui in self.hui_sets:
                if label == hui.itemset:
                    return
            self.hui_sets.append(HUI(label, node.fitness))

    # ── Output ───────────────────────────────

    def _write_out(self):
        lines = []
        for hui in self.hui_sets:
            lines.append(f"{hui.itemset}#UTIL: {hui.fitness}")
        with open(self.output_path, 'w') as f:
            f.write('\n'.join(lines))
            f.write('\n')

    def _check_memory(self):
        # Python doesn't expose JVM memory; just track a placeholder
        pass

    def print_stats(self):
        elapsed_ms = int((self.end_timestamp - self.start_timestamp) * 1000)
        print("=============  HUIM-GA ALGORITHM v.2.11 - STATS =============")
        print(f" Total time ~ {elapsed_ms} ms")
        print(f" Memory ~ N/A (Python runtime)")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")

    def get_results(self) -> List[HUI]:
        """Return discovered HUIs for programmatic access."""
        return self.hui_sets


# ─────────────────────────────────────────────
#  Verification helper
# ─────────────────────────────────────────────

def verify_against_java_output(java_output_path: str, python_hui_sets: List[HUI]) -> bool:
    """
    Compare Python results against a Java-produced output file.
    The Java output format is:  <items> #UTIL: <value>
    Returns True if both sets match exactly (same itemsets, same utilities).
    """
    if not os.path.exists(java_output_path):
        print(f"[verify] Java output file not found: {java_output_path}")
        return False

    java_results = {}
    with open(java_output_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if '#UTIL:' in line:
                parts = line.split('#UTIL:')
                itemset = parts[0].strip()
                utility = int(parts[1].strip())
                java_results[itemset] = utility

    python_results = {h.itemset.strip(): h.fitness for h in python_hui_sets}

    java_keys = set(java_results.keys())
    python_keys = set(python_results.keys())

    missing_in_python = java_keys - python_keys
    extra_in_python = python_keys - java_keys

    utility_mismatches = {
        k for k in java_keys & python_keys
        if java_results[k] != python_results[k]
    }

    if not missing_in_python and not extra_in_python and not utility_mismatches:
        print("[verify] ✅ MATCH — Python output is 100% identical to Java output.")
        return True
    else:
        print("[verify] ❌ MISMATCH detected:")
        if missing_in_python:
            print(f"  Missing in Python ({len(missing_in_python)}): {missing_in_python}")
        if extra_in_python:
            print(f"  Extra in Python   ({len(extra_in_python)}): {extra_in_python}")
        if utility_mismatches:
            print(f"  Utility differs   ({len(utility_mismatches)}):")
            for k in utility_mismatches:
                print(f"    '{k}'  Java={java_results[k]}  Python={python_results[k]}")
        return False


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────

def main():
    # Change to script's directory to ensure files are found
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    parser = argparse.ArgumentParser(
        description="HUIM-GA — High Utility Itemset Mining via Genetic Algorithm"
    )
    parser.add_argument('--input',       default='contextHUIM.txt',
                        help='Path to the input transaction database (default: contextHUIM.txt)')
    parser.add_argument('--output',      default='output_python.txt',
                        help='Path to write discovered HUIs (default: output_python.txt)')
    parser.add_argument('--min_utility', type=int, default=30,
                        help='Minimum utility threshold (default: 30)')
    parser.add_argument('--java_output', default=None,
                        help='Optional: path to Java output file for verification')
    parser.add_argument('--seed',        type=int, default=None,
                        help='Optional random seed for reproducibility')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        print(f"[info] Random seed set to {args.seed}")

    print(f"[info] Input        : {args.input}")
    print(f"[info] Output       : {args.output}")
    print(f"[info] Min utility  : {args.min_utility}")
    print()

    algo = AlgoHUIM_GA()
    algo.run_algorithm(args.input, args.output, args.min_utility)
    algo.print_stats()

    print()
    print("─── Discovered High-Utility Itemsets ───────────────────────────")
    results = algo.get_results()
    if results:
        for h in results:
            print(f"  {h.itemset.strip()}  #UTIL: {h.fitness}")
    else:
        print("  (none found)")
    print(f"────────────────────────────────────────────────────────────────")
    print(f"  Total: {len(results)} HUI(s) written to '{args.output}'")

    # Optional verification against Java output
    if args.java_output:
        print()
        verify_against_java_output(args.java_output, results)


if __name__ == '__main__':
    main()