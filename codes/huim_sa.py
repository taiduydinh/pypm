
import random
import math
import time
import os
from copy import deepcopy

# ─────────────────────────────────────────────
#  CONFIGURATION  ← edit these values
# ─────────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"   # path to input file
OUTPUT_FILE = "Python.output.txt"        # path to output file
MIN_UTILITY = 20                  # minimum utility threshold
# ─────────────────────────────────────────────


class Pair:
    """Represents an item and its utility in a transaction."""
    def __init__(self, item=0, utility=0):
        self.item    = item
        self.utility = utility


class ChroNode:
    """Represents a chromosome (candidate itemset)."""
    def __init__(self, length=0):
        self.chromosome = [False] * length   # bit array
        self.fitness    = 0
        self.rfitness   = 0.0
        self.rank       = 0

    def deepcopy(self, other):
        self.chromosome = other.chromosome[:]
        self.fitness    = other.fitness
        self.rfitness   = other.rfitness
        self.rank       = other.rank

    def cardinality(self):
        return sum(self.chromosome)

    def length(self):
        return len(self.chromosome)

    def calculate_fitness(self, k, trans_list, database, twu_pattern):
        """Calculate utility of the itemset across matching transactions."""
        if k == 0:
            return
        fitness = 0
        for p in trans_list:
            i = 0   # index into chromosome
            q = 0   # index into transaction items
            temp = 0
            s    = 0
            while q < len(database[p]) and i < len(self.chromosome):
                if self.chromosome[i]:
                    if database[p][q].item == twu_pattern[i]:
                        s    += database[p][q].utility
                        i    += 1
                        q    += 1
                        temp += 1
                    else:
                        q += 1
                else:
                    i += 1
            if temp == k:
                fitness += s
        self.fitness = fitness


class HUI:
    """Stores a discovered high-utility itemset."""
    def __init__(self, itemset, fitness):
        self.itemset = itemset
        self.fitness = fitness


class Item:
    """Bitmap representation of an item (which transactions it appears in)."""
    def __init__(self, item, db_size):
        self.item = item
        self.tids = [False] * db_size   # transaction id bitset


# ══════════════════════════════════════════════════════════════════════════════
class AlgoHUIMSA:
    POP_SIZE    = 30
    TEMPERATURE = 100_000.0
    MIN_TEMP    = 0.00001
    ALPHA       = 0.9993

    def __init__(self):
        self.max_memory       = 0.0
        self.start_time       = 0
        self.end_time         = 0
        self.transaction_count = 0

        self.map_item_to_twu  = {}
        self.map_item_to_twu0 = {}
        self.twu_pattern      = []

        self.database         = []    # List[List[Pair]]
        self.percentage       = []
        self.items            = []    # List[Item]

        self.population       = []
        self.sub_population   = []
        self.hui_ba           = []
        self.hui_sets         = []
        self.percent_hui_chro_node = []

    # ──────────────────────────────────────────
    def run_algorithm(self, input_path, output_path, min_utility):
        self.max_memory = 0.0
        self.start_time = time.time()

        # ── PASS 1: compute TWU for each item ──
        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(':')
                items_str = parts[0].strip().split()
                trans_utility = int(parts[1].strip())
                for it in items_str:
                    item = int(it)
                    self.map_item_to_twu[item]  = self.map_item_to_twu.get(item, 0)  + trans_utility
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + trans_utility

        # ── PASS 2: build database (keep only promising items) ──
        with open(input_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(':')
                items_str   = parts[0].strip().split()
                utility_str = parts[2].strip().split()

                revised = []
                for i, it in enumerate(items_str):
                    item    = int(it)
                    utility = int(utility_str[i])
                    if self.map_item_to_twu.get(item, 0) >= min_utility:
                        revised.append(Pair(item, utility))
                    else:
                        self.map_item_to_twu0.pop(item, None)
                self.database.append(revised)

        # sorted list of promising items
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        # ── Build bitmap (Items) ──
        self.items = [Item(it, len(self.database)) for it in self.twu_pattern]
        for i, trans in enumerate(self.database):
            for j, item_obj in enumerate(self.items):
                for pair in trans:
                    if item_obj.item == pair.item:
                        item_obj.tids[i] = True

        # ── SA main loop ──
        if self.twu_pattern:
            self._pop_init(min_utility)
            T     = self.TEMPERATURE
            T_min = self.MIN_TEMP

            while T > T_min:
                self.sub_population = self._neighbor(min_utility)
                T *= self.ALPHA

                if self.hui_ba:
                    pct  = self._roulette_percent_hui_ba()
                    num1 = self._roulette_select_hui_ba(pct)
                    num2 = self._roulette_select_hui_ba(pct)
                    tA   = random.randint(0, self.POP_SIZE - 1)
                    tB   = random.randint(0, self.POP_SIZE - 1)
                    self.population[tA].deepcopy(self.hui_ba[num1])
                    self.population[tB].deepcopy(self.hui_ba[num2])
                    num3 = self._roulette_select_hui_ba(pct)
                    tC   = random.randint(0, self.POP_SIZE - 1)
                    self.population[tC].deepcopy(self.hui_ba[num3])

                self._calculate_rfitness()
                self.sub_population.extend(self.population)
                self._rank_data(self.sub_population)
                for j in range(len(self.population)):
                    self.population[j] = self.sub_population[j]
                self.sub_population = []

        # ── Write output ──
        self._write_out(output_path)
        self.end_time = time.time()

    # ──────────────────────────────────────────
    def _pop_init(self, min_utility):
        self.percentage = self._roulette_percent()
        i = 0
        while i < self.POP_SIZE:
            node = ChroNode(len(self.twu_pattern))
            k    = random.randint(0, len(self.twu_pattern) - 1)
            j    = 0
            while j < k:
                temp = self._select(self.percentage)
                if not node.chromosome[temp]:
                    node.chromosome[temp] = True
                    j += 1
            trans_list = []
            self._pev_check(node, trans_list)
            node.calculate_fitness(k, trans_list, self.database, self.twu_pattern)
            node.rank = 0
            self.population.append(node)
            if node.fitness >= min_utility and node.cardinality() > 0:
                self._insert(node)
                self._add_hui_ba(node)
            i += 1

    # ──────────────────────────────────────────
    def _neighbor(self, min_utility):
        sub = []
        for i in range(self.POP_SIZE):
            temp_idx = random.randint(0, len(self.twu_pattern) - 1)
            # flip the bit
            self.population[i].chromosome[temp_idx] = not self.population[i].chromosome[temp_idx]

            k          = self.population[i].cardinality()
            trans_list = []
            self._pev_check(self.population[i], trans_list)
            self.population[i].calculate_fitness(k, trans_list, self.database, self.twu_pattern)

            if self.population[i].fitness >= min_utility and self.population[i].cardinality() > 0:
                self._insert(self.population[i])
                self._add_hui_ba(self.population[i])
            else:
                ar = self._acceptance_probability(self.TEMPERATURE)
                if ar > self._range(2.8, 3.2):
                    self._insert(self.population[i])
                    self._add_hui_ba(self.population[i])
        return sub

    # ──────────────────────────────────────────
    def _pev_check(self, node, lst):
        """Check item coverage; trim items that have no transactions."""
        set_bits = [i for i, b in enumerate(node.chromosome) if b]
        if not set_bits:
            return False

        tids = self.items[set_bits[0]].tids[:]   # copy
        mid  = tids[:]

        for i in range(1, len(set_bits)):
            new_tids = [a and b for a, b in zip(tids, self.items[set_bits[i]].tids)]
            if any(new_tids):
                tids = new_tids
                mid  = tids[:]
            else:
                tids = mid[:]
                node.chromosome[set_bits[i]] = False

        if not any(tids):
            return False
        lst.extend(idx for idx, v in enumerate(tids) if v)
        return True

    # ──────────────────────────────────────────
    def _insert(self, node):
        key = ' '.join(str(self.twu_pattern[i]) for i, b in enumerate(node.chromosome) if b) + ' '
        if not self.hui_sets:
            self.hui_sets.append(HUI(key, node.fitness))
        else:
            for h in self.hui_sets:
                if h.itemset == key:
                    return
            self.hui_sets.append(HUI(key, node.fitness))

    def _add_hui_ba(self, node):
        tmp = ChroNode(len(node.chromosome))
        tmp.deepcopy(node)
        for h in self.hui_ba:
            xor = [a ^ b for a, b in zip(tmp.chromosome, h.chromosome)]
            if not any(xor):
                return
        self.hui_ba.append(tmp)

    # ──────────────────────────────────────────
    def _roulette_percent(self):
        total  = sum(self.map_item_to_twu[it] for it in self.twu_pattern)
        pct    = []
        tmp    = 0.0
        for it in self.twu_pattern:
            tmp += self.map_item_to_twu[it]
            pct.append(tmp / total)
        return pct

    def _select(self, pct):
        r = random.random()
        for i, p in enumerate(pct):
            if i == 0:
                if 0 <= r <= p:
                    return 0
            elif pct[i-1] < r <= p:
                return i
        return len(pct) - 1

    def _roulette_percent_hui_ba(self):
        total = sum(h.fitness for h in self.hui_ba)
        pct   = []
        tmp   = 0.0
        for h in self.hui_ba:
            tmp += h.fitness
            pct.append(tmp / total)
        return pct

    def _roulette_select_hui_ba(self, pct):
        r = random.random()
        for i, p in enumerate(pct):
            if i == 0:
                if 0 <= r <= p:
                    return 0
            elif pct[i-1] < r <= p:
                return i
        return len(pct) - 1

    def _calculate_rfitness(self):
        total = sum(ind.fitness for ind in self.population) or 1
        tmp   = 0
        for ind in self.population:
            tmp += ind.fitness
            ind.rfitness = tmp / total

    def _rank_data(self, pop):
        pop.sort(key=lambda x: -x.fitness)
        for i in range(len(pop) - 1):
            pop[i].rank = i + 1

    def _range(self, mx, mn):
        return random.random() * (mx - mn) + mn

    def _acceptance_probability(self, temp):
        return math.exp(temp / (1 + temp))

    # ──────────────────────────────────────────
    def _write_out(self, output_path):
        lines = []
        for h in self.hui_sets:
            lines.append(f"{h.itemset}#UTIL: {h.fitness}")
        with open(output_path, 'w') as f:
            f.write('\n'.join(lines))

    def print_stats(self):
        elapsed_ms = (self.end_time - self.start_time) * 1000
        print("=============  HUIM-SA ALGORITHM - STATS =============")
        print(f" Total time ~ {elapsed_ms:.0f} ms")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")
        print(f"\nDiscovered High-Utility Itemsets (min_utility={MIN_UTILITY}):")
        print("-" * 50)
        for h in self.hui_sets:
            print(f"  {h.itemset.strip()}   #UTIL: {h.fitness}")
        print("-" * 50)
        print(f"Total HUIs found: {len(self.hui_sets)}")


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Resolve paths relative to this script's directory
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    input_path   = os.path.join(script_dir, INPUT_FILE)
    output_path  = os.path.join(script_dir, OUTPUT_FILE)

    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        exit(1)

    algo = AlgoHUIMSA()
    algo.run_algorithm(input_path, output_path, MIN_UTILITY)
    algo.print_stats()
    print(f"\nResults also written to: {output_path}")