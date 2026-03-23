"""
Python implementation of the Bio_HUIF_GA Algorithm for High-Utility Itemset Mining.
Converted from Java source by Wei Song, Chaomin Huang.

Reference:
  Wei Song, Chaomin Huang. Mining High Utility Itemsets Using Bio-Inspired Algorithms:
  A Diverse Optimal Value Framework. IEEE Access, 2018, 6(1): 19568-19582.

Usage:
  Set MIN_UTILITY below and run this file directly (press Play in VS Code).
  Output is written to output.txt and a summary is printed to the console.
"""

import random
import time
import os
import copy

# ─────────────────────────────────────────────
#  USER-CONFIGURABLE PARAMETERS
# ─────────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"   # path to the input database
OUTPUT_FILE = "python.output"     # path to write HUIs
MIN_UTILITY = 20                  # minimum utility threshold (change this to test)

POP_SIZE = 100    # population size  (matches Java: pop_size = 100)
MAX_ITER = 2000   # max iterations   (matches Java: max_iter = 2000)
# ─────────────────────────────────────────────


# ── Data classes ─────────────────────────────

class Pair:
    """Item + its utility in one transaction."""
    def __init__(self, item=0, utility=0):
        self.item    = item
        self.utility = utility


class HUI:
    """A discovered high-utility itemset."""
    def __init__(self, itemset: str, fitness: int):
        self.itemset = itemset
        self.fitness = fitness


class ChroNode:
    """
    Chromosome for the GA.
    chromosome : list of bool  (index → TWU-pattern position)
    fitness    : utility value of the represented itemset
    rfitness   : cumulative selection probability
    rank       : rank in sorted population
    """
    def __init__(self, length: int = 0):
        self.chromosome = [False] * length
        self.fitness    = 0
        self.rfitness   = 0.0
        self.rank       = 0

    # deep-copy from another ChroNode
    def deepcopy_from(self, other: "ChroNode"):
        self.chromosome = other.chromosome[:]
        self.fitness    = other.fitness
        self.rfitness   = other.rfitness
        self.rank       = other.rank

    def cardinality(self) -> int:
        return sum(self.chromosome)

    def length(self) -> int:
        """Index of the highest True bit + 1  (mirrors Java BitSet.length())."""
        for i in range(len(self.chromosome) - 1, -1, -1):
            if self.chromosome[i]:
                return i + 1
        return 0

    def calculate_fitness(self, k: int, trans_list: list,
                          database, twu_pattern):
        """
        Calculate the utility of the itemset encoded in this chromosome.
        k          : number of set bits (items in the itemset)
        trans_list : transaction indices where the itemset might appear
        """
        if k == 0:
            return
        fitness = 0
        for p in trans_list:
            i = 0          # position in chromosome
            q = 0          # position in transaction
            temp = 0       # matched item count
            total = 0      # accumulated utility
            tx = database[p]
            chrom_len = self.length()
            while q < len(tx) and i < chrom_len:
                if self.chromosome[i]:
                    if tx[q].item == twu_pattern[i]:
                        total += tx[q].utility
                        i += 1
                        q += 1
                        temp += 1
                    else:
                        q += 1
                else:
                    i += 1
            if temp == k:
                fitness += total
        self.fitness = fitness

    # comparison for sorting (descending fitness  → mirrors Java compareTo)
    def __lt__(self, other):
        return self.fitness > other.fitness   # reversed so sort() gives descending


class Item:
    """Bitmap representation of an item across transactions."""
    def __init__(self, item: int, db_size: int):
        self.item = item
        self.tids = [False] * db_size   # bit-array over transactions


# ── Algorithm ────────────────────────────────

class AlgoBio_HUIF_GA:

    def __init__(self):
        self.max_memory       = 0.0
        self.start_time       = 0
        self.end_time         = 0
        self.transaction_count= 0

        self.map_item_to_twu  = {}    # item → TWU  (pruned during pass-2)
        self.map_item_to_twu0 = {}    # item → TWU  (kept for all passing items)
        self.twu_pattern      = []    # sorted list of promising items

        self.database         = []    # list of list of Pair
        self.items            = []    # list of Item (bitmap per item)
        self.percentage       = []    # roulette percentages for item selection

        self.population       = []    # list of ChroNode
        self.sub_population   = []    # offspring
        self.hui_sets         = []    # discovered HUIs  (list of HUI)
        self.hui_ba           = []    # elite HUI chromosomes (list of ChroNode)
        self.percent_hui_chro_node = []

    # ── Public entry point ───────────────────

    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.max_memory = 0.0
        self.start_time = time.time()

        self.map_item_to_twu  = {}
        self.map_item_to_twu0 = {}

        # ── PASS 1: compute TWU for each item ──
        with open(input_path, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts            = line.split(":")
                items_str        = parts[0].strip().split()
                transaction_util = int(parts[1].strip())
                for it in items_str:
                    item = int(it)
                    self.map_item_to_twu[item]  = self.map_item_to_twu.get(item, 0)  + transaction_util
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + transaction_util

        # ── PASS 2: build revised database ──
        with open(input_path, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts          = line.split(":")
                items_str      = parts[0].strip().split()
                utility_values = parts[2].strip().split()

                revised_tx = []
                for i, it in enumerate(items_str):
                    p = Pair(item=int(it), utility=int(utility_values[i]))
                    if self.map_item_to_twu.get(p.item, 0) >= min_utility:
                        revised_tx.append(p)
                    else:
                        self.map_item_to_twu0.pop(p.item, None)
                self.database.append(revised_tx)

        # sorted list of promising items
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        # ── Build bitmap ──
        db_size = len(self.database)
        self.items = [Item(item, db_size) for item in self.twu_pattern]

        for i, tx in enumerate(self.database):
            for j, it_obj in enumerate(self.items):
                for pair in tx:
                    if it_obj.item == pair.item:
                        it_obj.tids[i] = True

        # ── GA ──
        if self.twu_pattern:
            m = len(self.database)
            n = len(self.twu_pattern)
            self._pop_init(min_utility)

            if m > n:
                p_min = 1.0 / m
                p_max = 1.0 / n
            else:
                p_min = 1.0 / n
                p_max = 1.0 / m

            for i in range(MAX_ITER):
                if len(self.hui_ba) > 2:
                    pct = self._roulette_percent_hui_ba()
                    num1 = self._roulette_select_hui_ba(pct)
                    num2 = self._roulette_select_hui_ba(pct)
                    temp_a = int(random.random() * POP_SIZE)
                    temp_b = int(random.random() * POP_SIZE)
                    self.population[temp_a].deepcopy_from(self.hui_ba[num1])
                    self.population[temp_b].deepcopy_from(self.hui_ba[num2])

                self._calculate_rfitness()

                while len(self.sub_population) < POP_SIZE:
                    t1 = self._select_chromosome()
                    t2 = self._select_chromosome()
                    while t1 == t2:
                        t2 = (t2 + int(random.random() * 1000)) % POP_SIZE
                    self._crossover(t1, t2, min_utility)

                self.sub_population = self._ranked_mutation(p_max, p_min, i, min_utility)
                self.sub_population.extend(self.population)
                self.sub_population.sort()          # descending fitness
                for j in range(len(self.population)):
                    self.population[j] = self.sub_population[j]
                self.sub_population.clear()

                if i % 200 == 0:
                    print(f"{i}-update end. HUIs No. is {len(self.hui_sets)}")

        self._write_out(output_path)
        self.end_time = time.time()

    # ── Population initialisation ────────────

    def _pop_init(self, min_utility: int):
        self.percentage = self._roulette_percent()
        i = 0
        while i < POP_SIZE:
            node  = ChroNode(len(self.twu_pattern))
            j     = 0
            k     = int(random.random() * len(self.twu_pattern))
            while j < k:
                temp = self._select(self.percentage)
                if not node.chromosome[temp]:
                    j += 1
                    node.chromosome[temp] = True

            trans_list = []
            self._pev_check(node, trans_list)
            node.calculate_fitness(k, trans_list, self.database, self.twu_pattern)
            node.rank = 0
            self.population.append(node)
            if node.fitness >= min_utility and node.cardinality() > 0:
                self._insert(node)
                self._add_hui_ba(node)
            i += 1

    # ── PEV pruning ──────────────────────────

    def _pev_check(self, node: ChroNode, out_list: list) -> bool:
        """
        Prune unpromising items from the chromosome (mirrors Java pev_Check).
        Fills out_list with transaction indices where the itemset is present.
        Returns True if the chromosome is non-empty after pruning.
        """
        set_bits = [i for i in range(node.length()) if node.chromosome[i]]
        if not set_bits:
            return False

        db_size = len(self.database)
        temp_bitset = self.items[set_bits[0]].tids[:]
        mid_bitset  = temp_bitset[:]

        for idx in range(1, len(set_bits)):
            # AND with next item's TID-list
            new_bitset = [a and b for a, b in zip(temp_bitset, self.items[set_bits[idx]].tids)]
            if any(new_bitset):
                temp_bitset = new_bitset
                mid_bitset  = new_bitset[:]
            else:
                # this item makes the intersection empty → remove it
                temp_bitset = mid_bitset[:]
                node.chromosome[set_bits[idx]] = False

        if not any(temp_bitset):
            return False

        for m in range(len(temp_bitset)):
            if temp_bitset[m]:
                out_list.append(m)
        return True

    # ── Fitness / selection helpers ──────────

    def _calculate_rfitness(self):
        total = sum(c.fitness for c in self.population)
        running = 0
        for c in self.population:
            running += c.fitness
            c.rfitness = running / (total + 1e-9)

    def _select_chromosome(self) -> int:
        rand = random.random()
        for i, c in enumerate(self.population):
            if i == 0:
                if 0 <= rand <= c.rfitness:
                    return 0
            elif self.population[i - 1].rfitness < rand <= c.rfitness:
                return i
        return 0

    def _roulette_percent(self):
        total    = sum(self.map_item_to_twu[it] for it in self.twu_pattern)
        pct      = []
        running  = 0.0
        for it in self.twu_pattern:
            running += self.map_item_to_twu[it]
            pct.append(running / (total + 1e-9))
        return pct

    def _select(self, pct: list) -> int:
        rand = random.random()
        for i, p in enumerate(pct):
            if i == 0:
                if 0 <= rand <= p:
                    return 0
            elif pct[i - 1] < rand <= p:
                return i
        return 0

    def _roulette_percent_hui_ba(self):
        total   = sum(c.fitness for c in self.hui_ba)
        pct     = []
        running = 0.0
        for c in self.hui_ba:
            running += c.fitness
            pct.append(running / (total + 1e-9))
        return pct

    def _roulette_select_hui_ba(self, pct: list) -> int:
        rand = random.random()
        for i, p in enumerate(pct):
            if i == 0:
                if 0 <= rand <= p:
                    return 0
            elif pct[i - 1] < rand <= p:
                return i
        return 0

    # ── Crossover ────────────────────────────

    def _bit_diff(self, a: ChroNode, b: ChroNode):
        """XOR of two chromosomes → list of differing positions."""
        size = max(len(a.chromosome), len(b.chromosome))
        ca   = a.chromosome + [False] * (size - len(a.chromosome))
        cb   = b.chromosome + [False] * (size - len(b.chromosome))
        return [i for i in range(size) if ca[i] != cb[i]]

    def _crossover(self, t1: int, t2: int, min_utility: int):
        node1 = ChroNode(len(self.twu_pattern))
        node2 = ChroNode(len(self.twu_pattern))
        node1.deepcopy_from(self.population[t1])
        node2.deepcopy_from(self.population[t2])

        dis_list = self._bit_diff(node1, node2)

        if dis_list:
            num1 = int(len(dis_list) * random.random()) + 1
            num2 = int(len(dis_list) * random.random()) + 1
            for _ in range(num1):
                bit = dis_list[int(len(dis_list) * random.random())]
                node1.chromosome[bit] = not node1.chromosome[bit]
            for _ in range(num2):
                bit = dis_list[int(len(dis_list) * random.random())]
                node2.chromosome[bit] = not node2.chromosome[bit]

        for node in (node1, node2):
            tl = []
            self._pev_check(node, tl)
            k = node.cardinality()
            node.calculate_fitness(k, tl, self.database, self.twu_pattern)
            node.rank     = 0
            node.rfitness = 0.0
            self.sub_population.append(node)
            if node.fitness >= min_utility and k > 0:
                self._insert(node)
                self._add_hui_ba(node)

    # ── Mutation ─────────────────────────────

    def _rank_data(self, pop: list):
        pop.sort()
        for i in range(len(pop) - 1):
            pop[i].rank = i + 1

    def _get_rank(self, pop: list):
        pop.sort()
        return list(range(1, len(self.sub_population) + 1))

    def _ranked_mutation(self, p_max: float, p_min: float,
                         current_iter: int, min_utility: int):
        record = self._get_rank(self.sub_population)
        for i in range(POP_SIZE):
            pm = (p_max - (p_max - p_min) * current_iter / MAX_ITER) \
                 * record[i] / len(self.sub_population)
            # Java always mutates (condition forced to true)
            temp = int(random.random() * len(self.twu_pattern))
            self.sub_population[i].chromosome[temp] = \
                not self.sub_population[i].chromosome[temp]

            k = self.sub_population[i].cardinality()
            tl = []
            self._pev_check(self.sub_population[i], tl)
            self.sub_population[i].calculate_fitness(
                k, tl, self.database, self.twu_pattern)
            if self.sub_population[i].fitness >= min_utility and k > 0:
                self._insert(self.sub_population[i])
                self._add_hui_ba(self.sub_population[i])
        return self.sub_population

    # ── HUI bookkeeping ──────────────────────

    def _insert(self, node: ChroNode):
        label = " ".join(
            str(self.twu_pattern[i])
            for i in range(len(self.twu_pattern))
            if node.chromosome[i]
        ) + " "
        if not self.hui_sets:
            self.hui_sets.append(HUI(label, node.fitness))
            return
        for h in self.hui_sets:
            if h.itemset == label:
                return
        self.hui_sets.append(HUI(label, node.fitness))

    def _add_hui_ba(self, node: ChroNode):
        new_node = ChroNode(len(self.twu_pattern))
        new_node.deepcopy_from(node)
        for existing in self.hui_ba:
            # check XOR == 0  (identical chromosome)
            if all(a == b for a, b in zip(new_node.chromosome, existing.chromosome)):
                return
        self.hui_ba.append(new_node)

    # ── Output ───────────────────────────────

    def _write_out(self, output_path: str):
        lines = []
        for h in self.hui_sets:
            lines.append(f"{h.itemset}#UTIL: {h.fitness}")
        with open(output_path, "w") as fh:
            fh.write("\n".join(lines))

    def print_stats(self):
        elapsed_ms = int((self.end_time - self.start_time) * 1000)
        print("=============  HUIF-GA ALGORITHM v.2.36 - STATS =============")
        print(f" Total time ~ {elapsed_ms} ms")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")


# ── Main ─────────────────────────────────────

if __name__ == "__main__":
    # Resolve paths relative to this script's directory
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    input_path   = os.path.join(script_dir, INPUT_FILE)
    output_path  = os.path.join(script_dir, OUTPUT_FILE)

    print(f"Running HUIF-GA  |  min_utility = {MIN_UTILITY}")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}\n")

    algo = AlgoBio_HUIF_GA()
    algo.run_algorithm(input_path, output_path, MIN_UTILITY)
    algo.print_stats()

    # ── Print discovered HUIs to console ──
    print(f"\nDiscovered High-Utility Itemsets (min_utility = {MIN_UTILITY}):")
    print("-" * 50)
    if algo.hui_sets:
        for h in algo.hui_sets:
            print(f"  {h.itemset.strip()}  #UTIL: {h.fitness}")
    else:
        print("  (none found)")
    print("-" * 50)
    print(f"Total HUIs: {len(algo.hui_sets)}")