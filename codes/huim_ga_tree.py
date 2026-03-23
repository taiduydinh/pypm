

import time
import os

# ─────────────────────────────────────────
#  ▶  CONFIGURE HERE
# ─────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"    # must be in the same folder as this script
OUTPUT_FILE = "Python.output.txt"  # output written here
MIN_UTILITY = 10                   # change this to test different thresholds
# ─────────────────────────────────────────


# ── Exact replica of Java's java.util.Random (seed=1) ─────────────────────────
# Java uses a 48-bit linear congruential generator:
#   seed_new = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1)
#   nextDouble() = ( (next(26) << 27) + next(27) ) / 2^53
class JavaRandom:
    _MULTIPLIER = 0x5DEECE66D    # 25214903917
    _ADDEND     = 0xB            # 11
    _MASK       = (1 << 48) - 1  # 281474976710655

    def __init__(self, seed: int = 1):
        self._seed = (seed ^ self._MULTIPLIER) & self._MASK

    def _next(self, bits: int) -> int:
        self._seed = (self._seed * self._MULTIPLIER + self._ADDEND) & self._MASK
        return self._seed >> (48 - bits)

    def next_double(self) -> float:
        return (((self._next(26) << 27) + self._next(27)) / (1 << 53))


# ── Data structures ────────────────────────────────────────────────────────────
class Pair:
    def __init__(self, item: int = 0, utility: int = 0):
        self.item    = item
        self.utility = utility


class ChroNode:
    """Chromosome node (mirrors Java ChroNode)."""
    def __init__(self, length: int = 0):
        self.chromosome: list = [0] * length
        self.fitness:    int  = 0
        self.rfitness:   float = 0.0
        self.rank:       int  = 0


class HUI:
    def __init__(self, itemset: str, fitness: int):
        self.itemset = itemset   # e.g. "1 2 3 "  (trailing space, matches Java)
        self.fitness = fitness


class TreeNode:
    """OR/NOR-tree node (mirrors Java treeNode)."""
    def __init__(self, item: int = -1):
        self.item = item
        self.OR   = None   # type: TreeNode | None
        self.NOR  = None   # type: TreeNode | None


# ── Main algorithm class ───────────────────────────────────────────────────────
class AlgoHUIM_GA_tree:
    POP_SIZE   = 20    # matches Java: final int pop_size = 20
    ITERATIONS = 2000  # matches Java: final int iterations = 2000

    def __init__(self):
        self.max_memory:       float      = 0.0
        self.start_time:       float      = 0.0
        self.end_time:         float      = 0.0
        self.map_item_to_twu:  dict       = {}
        self.twu_pattern:      list       = []   # sorted promising 1-items
        self.database:         list       = []   # list[list[Pair]]
        self.hui_sets:         list       = []   # discovered HUIs
        self.population:       list       = []   # list[ChroNode]
        self.sub_population:   list       = []   # list[ChroNode]
        self.maximal_patterns: list       = []   # list[list[int]]
        self.or_nor_tree:      TreeNode   = None
        self.random:           JavaRandom = JavaRandom(1)  # fixed seed = 1

    # ── Public entry point ────────────────────────────────────────────────────
    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.start_time = time.time()
        self._check_memory()

        # ── Pass 1: compute TWU for every item ────────────────────────────────
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(":")
                items = parts[0].split()
                tu    = int(parts[1])
                for it in items:
                    item = int(it)
                    self.map_item_to_twu[item] = self.map_item_to_twu.get(item, 0) + tu

        # ── Pass 2: build database + collect maximal transaction patterns ─────
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts   = line.split(":")
                items   = parts[0].split()
                uvs     = parts[2].split()
                revised = []
                pattern = []
                for i, it in enumerate(items):
                    item = int(it)
                    util = int(uvs[i])
                    if self.map_item_to_twu.get(item, 0) >= min_utility:
                        revised.append(Pair(item, util))
                        pattern.append(item)
                self.database.append(revised)
                self._maximal_judge(pattern)

        # ── Build twuPattern: sorted keys, then filter those < minUtility ─────
        # Mirrors Java: new ArrayList(mapItemToTWU.keySet()); sort; remove <min
        self.twu_pattern = sorted(self.map_item_to_twu.keys())
        self.twu_pattern = [k for k in self.twu_pattern
                            if self.map_item_to_twu[k] >= min_utility]

        self._check_memory()

        # ── Run the GA ────────────────────────────────────────────────────────
        if self.twu_pattern:
            m = len(self.database)
            n = len(self.twu_pattern)

            self.or_nor_tree = self._initial_tree(self.twu_pattern)
            self._generate_pop(min_utility)

            # Adaptive mutation rate bounds (matches Java exactly)
            if m > n:
                p_min = 1.0 / m
                p_max = 1.0 / n
            else:
                p_min = 1.0 / n
                p_max = 1.0 / m

            for iteration in range(self.ITERATIONS):
                self._calculate_rfitness()

                # Selection + crossover until sub_population reaches pop_size
                while len(self.sub_population) < self.POP_SIZE:
                    t1 = self._select_chromosome()
                    t2 = self._select_chromosome()
                    while t1 == t2:
                        t2 = self._select_chromosome()
                    self._crossover(t1, t2, min_utility)

                # Ranked mutation
                self.sub_population = self._ranked_mutation(
                    p_max, p_min, iteration, min_utility
                )

                # Merge sub_population + population, keep best pop_size
                self.sub_population.extend(self.population)
                self._rank_data(self.sub_population)
                for j in range(len(self.population)):
                    self.population[j] = self.sub_population[j]
                self.sub_population.clear()

        self._write_out(output_path)
        self._check_memory()
        self.end_time = time.time()

    # ── maximalJudge ─────────────────────────────────────────────────────────
    # Maintains the set of maximal transaction patterns.
    # Exact mirror of Java maximalJudge (index-based loop + break behaviour).
    def _maximal_judge(self, pattern: list):
        if not self.maximal_patterns:
            self.maximal_patterns.append(pattern)
            return

        i = 0
        while i < len(self.maximal_patterns):
            temp_pattern = self.maximal_patterns[i]
            j = k = temp = 0
            while j < len(pattern) and k < len(temp_pattern):
                if   pattern[j] < temp_pattern[k]: j += 1
                elif pattern[j] > temp_pattern[k]: k += 1
                else:                               j += 1; k += 1; temp += 1
            if temp == len(pattern) or temp == len(temp_pattern):
                if len(pattern) > len(temp_pattern):   # pattern contains tempPattern
                    self.maximal_patterns.pop(i)
                    self.maximal_patterns.append(pattern)
                # else: equal or pattern is contained — do nothing
                break
            i += 1
        else:
            # Loop ended without break → no containment → add as new maximal
            self.maximal_patterns.append(pattern)

    # ── initialTree: build OR/NOR-tree from maximal patterns ──────────────────
    def _initial_tree(self, htwui_list: list) -> TreeNode:
        root = TreeNode(htwui_list[0])
        for mp in self.maximal_patterns:
            current = root
            j = 0   # position in mp
            k = 0   # position in htwui_list
            while k < len(htwui_list):
                if j < len(mp):
                    if mp[j] > htwui_list[k]:       # NOR branch
                        if current.NOR is None:
                            nxt = htwui_list[k + 1] if k + 1 < len(htwui_list) else -1
                            current.NOR = TreeNode(nxt)
                        k += 1
                        current = current.NOR
                    else:                            # OR branch
                        if current.OR is None:
                            nxt = htwui_list[k + 1] if k + 1 < len(htwui_list) else -1
                            current.OR = TreeNode(nxt)
                        k += 1; j += 1
                        current = current.OR
                else:                                # j == len(mp)
                    if current.NOR is None:
                        nxt = htwui_list[k + 1] if k + 1 < len(htwui_list) else -1
                        current.NOR = TreeNode(nxt)
                    current = current.NOR
                    k += 1
        return root

    # ── generatePop: initialise pop_size chromosomes ──────────────────────────
    def _generate_pop(self, min_utility: int):
        i = 0
        while i < self.POP_SIZE:
            node = ChroNode(len(self.twu_pattern))
            node = self._chromosome_initial(node)
            k    = sum(1 for b in node.chromosome if b == 1)
            node.fitness = self._fit_calculate(node.chromosome, k)
            node.rank    = 0
            self.population.append(node)
            if node.fitness >= min_utility:
                self._insert(node)
            i += 1

    # ── chromosomeInitial: initialise one chromosome using the OR/NOR-tree ─────
    def _chromosome_initial(self, node: ChroNode) -> ChroNode:
        current = self.or_nor_tree
        for i in range(len(self.twu_pattern)):
            if current.OR is None:
                # Only NOR branch → always 0, follow NOR
                node.chromosome[i] = 0
                current = current.NOR
            elif current.NOR is None:
                # Only OR branch → random 0/1, always follow OR
                bit = 1 if self.random.next_double() > 0.5 else 0
                node.chromosome[i] = bit
                current = current.OR
            else:
                # Both branches → first random decides OR vs NOR
                go_or = 1 if self.random.next_double() > 0.5 else 0
                if go_or == 1:
                    # OR sub-tree: random 0/1, follow OR
                    bit = 1 if self.random.next_double() > 0.5 else 0
                    node.chromosome[i] = bit
                    current = current.OR
                else:
                    # NOR sub-tree: always 0, follow NOR
                    node.chromosome[i] = 0
                    current = current.NOR
        return node

    # ── crossover: single-point crossover ─────────────────────────────────────
    # Deliberately replicates Java's reference aliasing: both sub_population
    # entries appended here point to the SAME ChroNode object, so the second
    # chromosome overwrites the first in-place (Java behaviour preserved).
    def _crossover(self, t1: int, t2: int, min_utility: int):
        temp_a = temp_b = 0
        chro1  = []
        chro2  = []
        node   = ChroNode()   # single object — mirrors Java's single tempNode

        position = int(self.random.next_double() * len(self.twu_pattern))

        for i in range(len(self.twu_pattern)):
            if i <= position:
                chro1.append(self.population[t2].chromosome[i])
                if chro1[i] == 1: temp_a += 1
                chro2.append(self.population[t1].chromosome[i])
                if chro2[i] == 1: temp_b += 1
            else:
                chro1.append(self.population[t1].chromosome[i])
                if chro1[i] == 1: temp_a += 1
                chro2.append(self.population[t2].chromosome[i])
                if chro2[i] == 1: temp_b += 1

        # First offspring
        node.chromosome = chro1
        node.fitness    = self._fit_calculate(chro1, temp_a)
        node.rank       = 0
        self.sub_population.append(node)        # append reference
        if node.fitness >= min_utility:
            self._insert(node)

        # Second offspring — mutates the SAME node object (Java aliasing bug)
        node.chromosome = chro2
        node.fitness    = self._fit_calculate(chro2, temp_b)
        node.rank       = 0
        self.sub_population.append(node)        # same reference again
        if node.fitness >= min_utility:
            self._insert(node)

    # ── rankData: selection sort descending by fitness (swaps contents) ────────
    def _rank_data(self, temp_pop: list):
        n = len(temp_pop)
        for i in range(n - 1):
            p = i
            for j in range(i + 1, n):
                if temp_pop[p].fitness < temp_pop[j].fitness:
                    p = j
            if i != p:
                temp_pop[i].fitness, temp_pop[p].fitness = (
                    temp_pop[p].fitness, temp_pop[i].fitness
                )
                for q in range(len(self.twu_pattern)):
                    temp_pop[i].chromosome[q], temp_pop[p].chromosome[q] = (
                        temp_pop[p].chromosome[q], temp_pop[i].chromosome[q]
                    )
            temp_pop[i].rank = i + 1
        if n > 0:
            temp_pop[n - 1].rank = n

    # ── getRank: count chromosomes that beat each sub_population entry ─────────
    def _get_rank(self) -> list:
        result = []
        size   = len(self.sub_population)
        for i in range(size):
            cnt = 0
            for j in range(size):
                if i != j and self.sub_population[i].fitness <= self.sub_population[j].fitness:
                    cnt += 1
            result.append(cnt + 1)
        return result

    # ── rankedMutation: rank-based adaptive single-point mutation ─────────────
    def _ranked_mutation(self, p_max: float, p_min: float,
                         current_iter: int, min_utility: int) -> list:
        record = self._get_rank()
        for i in range(self.POP_SIZE):
            pm       = ((p_max - (p_max - p_min) * current_iter / self.ITERATIONS)
                        * record[i] / len(self.sub_population))
            rank_num = self.random.next_double()
            if rank_num < pm:
                pos = int(self.random.next_double() * len(self.twu_pattern))
                if self.sub_population[i].chromosome[pos] == 1:
                    self.sub_population[i].chromosome[pos] = 0
                else:
                    self.sub_population[i].chromosome[pos] = 1
                k = sum(1 for b in self.sub_population[i].chromosome if b == 1)
                self.sub_population[i].fitness = self._fit_calculate(
                    self.sub_population[i].chromosome, k
                )
                if self.sub_population[i].fitness >= min_utility:
                    self._insert(self.sub_population[i])
        return self.sub_population

    # ── calculateRfitness: cumulative proportional fitness ────────────────────
    def _calculate_rfitness(self):
        total      = sum(nd.fitness for nd in self.population)
        cumulative = 0
        for nd in self.population:
            cumulative += nd.fitness
            # Mirrors Java: 0/0.0 = NaN (not an error)
            nd.rfitness = (cumulative / float(total)) if total != 0 else float('nan')

    # ── selectChromosome: roulette-wheel selection ────────────────────────────
    def _select_chromosome(self) -> int:
        rand_num = self.random.next_double()
        for i in range(len(self.population)):
            if i == 0:
                if 0 <= rand_num <= self.population[0].rfitness:
                    return 0
            elif (self.population[i - 1].rfitness < rand_num
                  <= self.population[i].rfitness):
                return i
        return 0   # default (mirrors Java: temp initialised to 0)

    # ── insert: add chromosome to hui_sets if not already present ─────────────
    def _insert(self, node: ChroNode):
        key = ""
        for i in range(len(self.twu_pattern)):
            if node.chromosome[i] == 1:
                key += str(self.twu_pattern[i]) + " "
        for h in self.hui_sets:
            if key == h.itemset:
                return
        self.hui_sets.append(HUI(key, node.fitness))

    # ── fitCalculate: exact utility of a chromosome across the database ────────
    def _fit_calculate(self, chromosome: list, k: int) -> int:
        if k == 0:
            return 0
        fitness = 0
        for p in range(len(self.database)):
            i = j = q = temp = s = 0
            while j < k and q < len(self.database[p]) and i < len(chromosome):
                if chromosome[i] == 1:
                    db = self.database[p][q].item
                    tw = self.twu_pattern[i]
                    if   db < tw:   q += 1
                    elif db == tw:  s += self.database[p][q].utility; j += 1; q += 1; temp += 1; i += 1
                    else:           j += 1; i += 1
                else:
                    i += 1
            if temp == k:
                fitness += s
        return fitness

    # ── writeOut: write discovered HUIs to output file ────────────────────────
    def _write_out(self, output_path: str):
        with open(output_path, "w") as f:
            for h in self.hui_sets:
                f.write(f"{h.itemset}#UTIL: {h.fitness}\n")
            f.write("\n")

    # ── checkMemory / printStats ──────────────────────────────────────────────
    def _check_memory(self):
        try:
            import tracemalloc
            if not tracemalloc.is_tracing():
                tracemalloc.start()
            current, _ = tracemalloc.get_traced_memory()
            mb = current / 1024 / 1024
            if mb > self.max_memory:
                self.max_memory = mb
        except Exception:
            pass

    def print_stats(self):
        elapsed_ms = int((self.end_time - self.start_time) * 1000)
        print("=============  HUIM-GA-tree ALGORITHM v.2.11 - STATS =============")
        print(f" Total time ~ {elapsed_ms} ms")
        print(f" Memory ~ {self.max_memory:.2f} MB")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    input_path  = os.path.join(script_dir, INPUT_FILE)
    output_path = os.path.join(script_dir, OUTPUT_FILE)

    print("\n" + "=" * 62)
    print("  HUIM-GA-tree  (Python — exact GA replica of Java version)")
    print("=" * 62)
    print(f"  Input      : {INPUT_FILE}")
    print(f"  Output     : {OUTPUT_FILE}")
    print(f"  MIN_UTILITY: {MIN_UTILITY}")
    print("=" * 62 + "\n")

    algo = AlgoHUIM_GA_tree()
    algo.run_algorithm(input_path, output_path, MIN_UTILITY)
    algo.print_stats()

    print(f"\n{'─'*62}")
    print(f"  HIGH-UTILITY ITEMSETS   (min_utility = {MIN_UTILITY})")
    print(f"{'─'*62}")
    if algo.hui_sets:
        for h in algo.hui_sets:
            print(f"  {h.itemset.strip():<30}  #UTIL: {h.fitness}")
    else:
        print("  (none found)")
    print(f"{'─'*62}")
    print(f"  Total HUIs found : {len(algo.hui_sets)}")
    print(f"  Output saved to  : {output_path}")
    print(f"{'─'*62}\n")