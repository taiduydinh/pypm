

import time
import random
import os
from copy import deepcopy

# ─────────────────────────────────────────────
#  ▶  CONFIGURATION — edit these three values
# ─────────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"   # path to your dataset
OUTPUT_FILE = "output_python.txt" # where results are written
MIN_UTILITY = 20                  # minimum utility threshold
# ─────────────────────────────────────────────


# ── Algorithm constants (mirror Java finals) ──
POP_SIZE = 30
MAX_ITER = 10000


# ══════════════════════════════════════════════
#  Data classes
# ══════════════════════════════════════════════

class Pair:
    """Item + its utility in one transaction."""
    __slots__ = ("item", "utility")
    def __init__(self, item=0, utility=0):
        self.item    = item
        self.utility = utility


class HUI:
    """A discovered high-utility itemset."""
    __slots__ = ("itemset", "fitness")
    def __init__(self, itemset: str, fitness: int):
        self.itemset  = itemset
        self.fitness  = fitness


class Item:
    """An item with its transaction-id bitset (represented as a Python set)."""
    __slots__ = ("item", "tids")
    def __init__(self, item=0):
        self.item = item
        self.tids: set = set()   # set of transaction indices


class ChroNode:
    """
    A chromosome = a BitSet over twuPattern indices.
    Stored as a Python set of integer indices (indices where bit=1).
    """
    def __init__(self):
        self.chromosome: set = set()   # set of bit positions that are 1
        self.fitness:    int  = 0
        self.rfitness: float  = 0.0
        self.rank:       int  = 0

    def deepcopy(self, other: "ChroNode"):
        self.chromosome = set(other.chromosome)
        self.fitness    = other.fitness
        self.rfitness   = other.rfitness
        self.rank       = other.rank

    def cardinality(self) -> int:
        return len(self.chromosome)

    def get(self, i: int) -> bool:
        return i in self.chromosome

    def set_bit(self, i: int):
        self.chromosome.add(i)

    def clear_bit(self, i: int):
        self.chromosome.discard(i)

    def length(self) -> int:
        """Highest set bit index + 1  (mirrors Java BitSet.length())."""
        return (max(self.chromosome) + 1) if self.chromosome else 0

    def calculate_fitness(self, k: int, trans_list: list,
                          database: list, twu_pattern: list):
        """Calculate fitness = total utility of the itemset across matching transactions."""
        if k == 0:
            self.fitness = 0
            return

        fitness = 0
        for p in trans_list:
            transaction = database[p]
            i = 0          # index into twuPattern (chromosome positions)
            q = 0          # index into transaction items
            temp = 0       # count of matched items
            total = 0      # sum of utilities

            chrom_len = self.length()
            while q < len(transaction) and i < chrom_len:
                if self.get(i):
                    if transaction[q].item == twu_pattern[i]:
                        total += transaction[q].utility
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


# ══════════════════════════════════════════════
#  Main algorithm class
# ══════════════════════════════════════════════

class AlgoHUIMHC:

    def __init__(self):
        self.max_memory       = 0.0
        self.start_timestamp  = 0
        self.end_timestamp    = 0
        self.transaction_count = 0

        self.map_item_to_twu:  dict = {}
        self.map_item_to_twu0: dict = {}
        self.twu_pattern:      list = []   # sorted list of promising items

        self.database:         list = []   # list[list[Pair]]
        self.percentage:       list = []   # roulette percentages for items
        self.items:            list = []   # list[Item]  — bitmap DB

        self.population:       list = []   # list[ChroNode]
        self.sub_population:   list = []   # list[ChroNode]
        self.hui_ba:           list = []   # list[ChroNode]  — HUI archive
        self.hui_sets:         list = []   # list[HUI]  — final results
        self.percent_hui_chro: list = []   # roulette percentages for huiBA

    # ── public entry point ────────────────────

    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.max_memory      = 0.0
        self.start_timestamp = int(time.time() * 1000)

        self.map_item_to_twu  = {}
        self.map_item_to_twu0 = {}

        # ── Pass 1: compute TWU for every item ──────────────────────────
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(":")
                items_str  = parts[0].strip().split()
                trans_util = int(parts[1].strip())
                for s in items_str:
                    item = int(s)
                    self.map_item_to_twu[item]  = self.map_item_to_twu.get(item, 0)  + trans_util
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + trans_util

        # ── Pass 2: build pruned database ───────────────────────────────
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts       = line.split(":")
                items_str   = parts[0].strip().split()
                utility_str = parts[2].strip().split()

                revised = []
                for i, s in enumerate(items_str):
                    item    = int(s)
                    utility = int(utility_str[i])
                    if self.map_item_to_twu.get(item, 0) >= min_utility:
                        revised.append(Pair(item, utility))
                    else:
                        self.map_item_to_twu0.pop(item, None)

                self.database.append(revised)

        # ── Build twuPattern (sorted promising items) ────────────────────
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        # ── Build bitmap (Item → set of transaction indices) ─────────────
        self.items = [Item(itm) for itm in self.twu_pattern]
        for i, transaction in enumerate(self.database):
            for j, itm_obj in enumerate(self.items):
                for pair in transaction:
                    if itm_obj.item == pair.item:
                        itm_obj.tids.add(i)

        self._check_memory()

        # ── Main hill-climbing loop ──────────────────────────────────────
        if self.twu_pattern:
            self._pop_init(min_utility)

            for _ in range(MAX_ITER):
                self.sub_population = self._neighbor(min_utility)

                if self.hui_ba:
                    pct = self._roulette_percent_hui_ba()

                    num1 = self._roulette_select_hui_ba(pct)
                    num2 = self._roulette_select_hui_ba(pct)
                    tA   = int(random.random() * POP_SIZE)
                    tB   = int(random.random() * POP_SIZE)
                    self.population[tA].deepcopy(self.hui_ba[num1])
                    self.population[tB].deepcopy(self.hui_ba[num2])

                    num3 = self._roulette_select_hui_ba(pct)
                    tC   = int(random.random() * POP_SIZE)
                    self.population[tC].deepcopy(self.hui_ba[num3])

                self._calculate_rfitness()

                self.sub_population.extend(self.population)
                self._rank_data(self.sub_population)
                for j in range(len(self.population)):
                    self.population[j] = self.sub_population[j]
                self.sub_population = []

        self._write_out(output_path)
        self._check_memory()
        self.end_timestamp = int(time.time() * 1000)

    # ── Statistics ────────────────────────────

    def print_stats(self):
        print("=============  HUIM-HC ALGORITHM - STATS =============")
        print(f" Total time ~ {self.end_timestamp - self.start_timestamp} ms")
        print(f" Memory ~ {self.max_memory:.2f} MB")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")

    # ── Private helpers ───────────────────────

    def _pop_init(self, min_utility: int):
        """Initialise population (mirrors pop_Init in Java)."""
        self.percentage = self._roulette_percent()
        i = 0
        while i < POP_SIZE:
            node = ChroNode()
            k    = int(random.random() * len(self.twu_pattern))
            j    = 0
            while j < k:
                temp = self._select(self.percentage)
                if not node.get(temp):
                    node.set_bit(temp)
                    j += 1

            trans_list: list = []
            self._pev_check(node, trans_list)
            node.calculate_fitness(k, trans_list, self.database, self.twu_pattern)
            node.rank = 0
            self.population.append(node)

            if node.fitness >= min_utility and node.cardinality() > 0:
                self._insert(node)
                self._add_hui_ba(node)
            i += 1

    def _neighbor(self, min_utility: int) -> list:
        """One iteration of hill-climbing neighbour generation."""
        for i in range(POP_SIZE):
            temp = int(random.random() * len(self.twu_pattern))
            if self.population[i].get(temp):
                self.population[i].clear_bit(temp)
            else:
                self.population[i].set_bit(temp)

            k          = self.population[i].cardinality()
            trans_list = []
            self._pev_check(self.population[i], trans_list)
            self.population[i].calculate_fitness(k, trans_list, self.database, self.twu_pattern)

            if self.population[i].fitness >= min_utility and self.population[i].cardinality() > 0:
                self._insert(self.population[i])
                self._add_hui_ba(self.population[i])

        return self.sub_population   # still empty at this point (matches Java)

    def _pev_check(self, node: ChroNode, out_list: list) -> bool:
        """
        Check whether the itemset has at least one supporting transaction.
        Fills out_list with transaction indices that contain ALL items in node.
        Mirrors pev_Check in Java (including the 'midBitSet' pruning logic).
        """
        temp_list = [i for i in range(node.length()) if node.get(i)]
        if not temp_list:
            return False

        # Intersect TID sets item by item; if empty, drop that item from chromosome
        current_tids = set(self.items[temp_list[0]].tids)
        mid_tids     = set(current_tids)

        for i in range(1, len(temp_list)):
            new_tids = current_tids & self.items[temp_list[i]].tids
            if new_tids:
                current_tids = new_tids
                mid_tids     = set(new_tids)
            else:
                current_tids = set(mid_tids)
                node.clear_bit(temp_list[i])

        if not current_tids:
            return False

        out_list.extend(sorted(current_tids))
        return True

    def _insert(self, node: ChroNode):
        """Add a HUI to hui_sets if not already present."""
        parts = [str(self.twu_pattern[i])
                 for i in range(len(self.twu_pattern)) if node.get(i)]
        key = " ".join(parts) + " "      # Java appends a trailing space

        if not self.hui_sets:
            self.hui_sets.append(HUI(key, node.fitness))
        else:
            for h in self.hui_sets:
                if h.itemset == key:
                    return
            self.hui_sets.append(HUI(key, node.fitness))

    def _add_hui_ba(self, node: ChroNode):
        """Add to HUI archive if not already present (XOR check)."""
        clone = ChroNode()
        clone.deepcopy(node)

        for existing in self.hui_ba:
            xor_bits = clone.chromosome.symmetric_difference(existing.chromosome)
            if len(xor_bits) == 0:
                return

        self.hui_ba.append(clone)

    def _rank_data(self, pop: list):
        """Sort population descending by fitness; assign ranks."""
        pop.sort(key=lambda x: -x.fitness)
        for i in range(len(pop) - 1):
            pop[i].rank = i + 1

    def _calculate_rfitness(self):
        """Compute cumulative relative fitness for population."""
        total = sum(n.fitness for n in self.population)
        running = 0
        for node in self.population:
            running += node.fitness
            node.rfitness = running / (total + 1e-9)

    def _roulette_percent(self) -> list:
        """Build cumulative TWU percentage list for initial population roulette."""
        total    = sum(self.map_item_to_twu[itm] for itm in self.twu_pattern)
        running  = 0.0
        result   = []
        for itm in self.twu_pattern:
            running += self.map_item_to_twu[itm]
            result.append(running / (total + 1e-9))
        return result

    def _select(self, pct: list) -> int:
        """Roulette-wheel select an index from percentage list."""
        r = random.random()
        for i, p in enumerate(pct):
            if i == 0:
                if 0 <= r <= p:
                    return 0
            elif pct[i - 1] < r <= p:
                return i
        return len(pct) - 1

    def _roulette_percent_hui_ba(self) -> list:
        """Build cumulative fitness percentage list for huiBA roulette."""
        total   = sum(n.fitness for n in self.hui_ba)
        running = 0.0
        result  = []
        for n in self.hui_ba:
            running += n.fitness
            result.append(running / (total + 1e-9))
        return result

    def _roulette_select_hui_ba(self, pct: list) -> int:
        """Roulette-wheel select index from huiBA percentage list."""
        return self._select(pct)

    def _write_out(self, output_path: str):
        """Write hui_sets to output file in the same format as Java."""
        lines = []
        for i, h in enumerate(self.hui_sets):
            lines.append(f"{h.itemset}#UTIL: {h.fitness}")
        with open(output_path, "w") as f:
            f.write("\n".join(lines))

    def _check_memory(self):
        """Approximate memory usage (Python does not expose JVM-style heap)."""
        try:
            import resource
            mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
        except ImportError:
            mem_mb = 0.0   # Windows — resource module unavailable
        if mem_mb > self.max_memory:
            self.max_memory = mem_mb


# ══════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════

if __name__ == "__main__":
    # Resolve input path relative to this script's directory
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    input_path   = os.path.join(script_dir, INPUT_FILE)
    output_path  = os.path.join(script_dir, OUTPUT_FILE)

    print(f"Input  : {input_path}")
    print(f"Output : {output_path}")
    print(f"Min utility : {MIN_UTILITY}")
    print()

    algo = AlgoHUIMHC()
    algo.run_algorithm(input_path, output_path, MIN_UTILITY)
    algo.print_stats()

    print()
    print("─── Discovered High-Utility Itemsets ───")
    if algo.hui_sets:
        for h in algo.hui_sets:
            print(f"  {h.itemset.strip()}  #UTIL: {h.fitness}")
    else:
        print("  (none found — try lowering MIN_UTILITY)")

    print()
    print(f"Results also written to: {output_path}")