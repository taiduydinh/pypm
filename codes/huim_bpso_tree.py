

import random
import math
import time
import os

# ─────────────────────────────────────────────
#  CONFIGURATION  ← change these values to test
# ─────────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"   # path to input file
OUTPUT_FILE = "output_python.txt" # path to output file
MIN_UTILITY = 20                  # minimum utility threshold
# ─────────────────────────────────────────────

# Algorithm parameters (must match Java)
POP_SIZE   = 20
ITERATIONS = 2000
C1, C2    = 2, 2
W         = 0.9


# ── Data structures ──────────────────────────

class TreeNode:
    """OR/NOR-tree node."""
    def __init__(self, item=-1):
        self.item = item
        self.OR   = None   # branch taken when item IS selected
        self.NOR  = None   # branch taken when item is NOT selected


class Particle:
    def __init__(self, length=0):
        self.X       = [0] * length   # binary position vector
        self.fitness = 0

    def copy_from(self, other):
        self.X       = list(other.X)
        self.fitness = other.fitness


class HUI:
    def __init__(self, itemset: str, fitness: int):
        self.itemset = itemset
        self.fitness = fitness


class Pair:
    def __init__(self, item=0, utility=0):
        self.item    = item
        self.utility = utility


# ── Algorithm ────────────────────────────────

class AlgoHUIM_BPSO_tree:

    def __init__(self):
        self.max_memory      = 0.0
        self.start_timestamp = 0
        self.end_timestamp   = 0

        self.map_item_to_twu  = {}   # TWU of every item seen
        self.map_item_to_twu0 = {}   # same, but items below minUtil are removed
        self.twu_pattern      = []   # items whose TWU >= minUtil (sorted)

        self.database          = []  # list of list of Pair
        self.maximal_patterns  = []  # list of list of int

        self.or_nor_tree = None      # root TreeNode

        self.population  = []        # list of Particle
        self.p_best      = []        # list of Particle (personal bests)
        self.g_best      = None      # Particle (global best)
        self.hui_sets    = []        # list of HUI
        self.V           = []        # list of list of float (velocities)

    # ── public entry point ──────────────────

    def run_algorithm(self, input_path: str, output_path: str, min_utility: int):
        self.max_memory      = 0.0
        self.start_timestamp = time.time()

        # ── Pass 1: compute TWU for every item ──
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts = line.split(":")
                items = parts[0].split()
                trans_utility = int(parts[1])
                for item_str in items:
                    item = int(item_str)
                    self.map_item_to_twu[item]  = self.map_item_to_twu.get(item, 0)  + trans_utility
                    self.map_item_to_twu0[item] = self.map_item_to_twu0.get(item, 0) + trans_utility

        # ── Pass 2: build database, find maximal patterns ──
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '%', '@'):
                    continue
                parts          = line.split(":")
                items          = parts[0].split()
                utility_values = parts[2].split()

                revised_transaction = []
                pattern             = []

                for i, item_str in enumerate(items):
                    item    = int(item_str)
                    utility = int(utility_values[i])
                    if self.map_item_to_twu.get(item, 0) >= min_utility:
                        revised_transaction.append(Pair(item, utility))
                        pattern.append(item)
                    else:
                        # remove item that didn't pass TWU pruning
                        self.map_item_to_twu0.pop(item, None)

                self.database.append(revised_transaction)
                self._maximal_judge(pattern)

        # items with TWU >= minUtil, sorted ascending
        self.twu_pattern = sorted(self.map_item_to_twu0.keys())

        # initialise pBest slots
        for _ in range(POP_SIZE):
            self.p_best.append(Particle(len(self.twu_pattern)))

        self.g_best = Particle(len(self.twu_pattern))

        self._check_memory()

        if self.twu_pattern:
            self.or_nor_tree = self._initial_tree(self.twu_pattern)
            self._generate_pop(min_utility)
            for _ in range(ITERATIONS):
                self._update(min_utility)

        self._write_out(output_path)
        self._check_memory()
        self.end_timestamp = time.time()

    # ── maximal pattern tracking ─────────────

    def _maximal_judge(self, pattern):
        if not pattern:
            return
        if not self.maximal_patterns:
            self.maximal_patterns.append(list(pattern))
            return

        i = 0
        while i < len(self.maximal_patterns):
            temp_pattern = self.maximal_patterns[i]
            j, k, temp = 0, 0, 0
            while j < len(pattern) and k < len(temp_pattern):
                if pattern[j] < temp_pattern[k]:
                    j += 1
                elif pattern[j] > temp_pattern[k]:
                    k += 1
                else:
                    j += 1; k += 1; temp += 1

            if temp == len(pattern) or temp == len(temp_pattern):
                if len(pattern) > len(temp_pattern):
                    # pattern contains tempPattern → replace
                    self.maximal_patterns.pop(i)
                    self.maximal_patterns.append(list(pattern))
                # else: equal or contained → do nothing
                return
            i += 1

        # no containing relationship → just add
        self.maximal_patterns.append(list(pattern))

    # ── OR/NOR-tree construction ─────────────

    def _initial_tree(self, htwui_list):
        root = TreeNode(htwui_list[0])

        for mp in self.maximal_patterns:
            current_node = root
            temp_pattern = mp
            j = 0   # position in temp_pattern
            k = 0   # position in htwui_list

            while k < len(htwui_list):
                if j < len(temp_pattern):
                    if temp_pattern[j] > htwui_list[k]:
                        # item htwui_list[k] NOT in maximal pattern → NOR branch
                        if current_node.NOR is None:
                            leaf = TreeNode(htwui_list[k + 1] if k + 1 < len(htwui_list) else -1)
                            current_node.NOR = leaf
                        k += 1
                        current_node = current_node.NOR
                    else:
                        # item matches → OR branch
                        if current_node.OR is None:
                            leaf = TreeNode(htwui_list[k + 1] if k + 1 < len(htwui_list) else -1)
                            current_node.OR = leaf
                        k += 1; j += 1
                        current_node = current_node.OR
                else:
                    # exhausted maximal pattern items → NOR branch for rest
                    if current_node.NOR is None:
                        leaf = TreeNode(htwui_list[k + 1] if k + 1 < len(htwui_list) else -1)
                        current_node.NOR = leaf
                    current_node = current_node.NOR
                    k += 1
        return root

    # ── Particle initialisation ──────────────

    def _particle_initial(self, particle: Particle) -> Particle:
        current_node = self.or_nor_tree
        for i in range(len(self.twu_pattern)):
            if current_node.OR is None:
                particle.X[i] = 0
                current_node  = current_node.NOR
            elif current_node.NOR is None:
                particle.X[i] = 1 if random.random() > 0.5 else 0
                current_node  = current_node.OR
            else:
                if random.random() > 0.5:   # OR branch
                    particle.X[i] = 1 if random.random() > 0.5 else 0
                    current_node  = current_node.OR
                else:                        # NOR branch
                    particle.X[i] = 0
                    current_node  = current_node.NOR
        return particle

    # ── Population generation ────────────────

    def _generate_pop(self, min_utility: int):
        for i in range(POP_SIZE):
            temp_particle = Particle(len(self.twu_pattern))
            temp_particle = self._particle_initial(temp_particle)
            k = sum(temp_particle.X)
            temp_particle.fitness = self._fit_calculate(temp_particle.X, k)
            self.population.append(temp_particle)

            self.p_best[i].copy_from(temp_particle)

            if temp_particle.fitness >= min_utility:
                self._insert(temp_particle)

            if i == 0:
                self.g_best.copy_from(self.p_best[i])
            else:
                if self.p_best[i].fitness > self.g_best.fitness:
                    self.g_best.copy_from(self.p_best[i])

            # initial velocities ~ Uniform(0,1)
            self.V.append([random.random() for _ in range(len(self.twu_pattern))])

    # ── Particle update ──────────────────────

    def _particle_update(self, temp_particle: Particle, i: int) -> Particle:
        current_particle = self.or_nor_tree
        for j in range(len(self.twu_pattern)):
            if current_particle.OR is None:
                temp_particle.X[j] = 0
                current_particle   = current_particle.NOR
            elif current_particle.NOR is None:
                sig = 1.0 / (1.0 + math.exp(-self.V[i][j]))
                temp_particle.X[j] = 1 if random.random() < sig else 0
                current_particle   = current_particle.OR
            else:
                if random.random() > 0.5:   # OR branch
                    sig = 1.0 / (1.0 + math.exp(-self.V[i][j]))
                    temp_particle.X[j] = 1 if random.random() < sig else 0
                    current_particle   = current_particle.OR
                else:                        # NOR branch
                    temp_particle.X[j] = 0
                    current_particle   = current_particle.NOR
        return temp_particle

    # ── BPSO update loop ─────────────────────

    def _update(self, min_utility: int):
        for i in range(POP_SIZE):
            k  = 0
            r1 = random.random()
            r2 = random.random()

            # velocity update
            for j in range(len(self.twu_pattern)):
                temp = (W * self.V[i][j]
                        + r1 * C1 * (self.p_best[i].X[j] - self.population[i].X[j])
                        + r2 * C2 * (self.g_best.X[j]    - self.population[i].X[j]))
                self.V[i][j] = max(-2.0, min(2.0, temp))

            # position update
            self.population[i] = self._particle_update(self.population[i], i)
            k = sum(self.population[i].X)

            # fitness
            self.population[i].fitness = self._fit_calculate(self.population[i].X, k)

            # update personal / global best
            if self.population[i].fitness > self.p_best[i].fitness:
                self.p_best[i].copy_from(self.population[i])
                if self.p_best[i].fitness > self.g_best.fitness:
                    self.g_best.copy_from(self.p_best[i])

            if self.population[i].fitness >= min_utility:
                self._insert(self.population[i])

    # ── Fitness calculation ──────────────────

    def _fit_calculate(self, temp_particle, k: int) -> int:
        if k == 0:
            return 0
        fitness = 0
        for trans in self.database:
            i = j = q = temp = 0
            total = 0
            while j < k and q < len(trans) and i < len(temp_particle):
                if temp_particle[i] == 1:
                    if trans[q].item < self.twu_pattern[i]:
                        q += 1
                    elif trans[q].item == self.twu_pattern[i]:
                        total += trans[q].utility
                        j += 1; q += 1; temp += 1; i += 1
                    else:   # trans[q].item > twu_pattern[i]
                        j += 1; i += 1
                else:
                    i += 1
            if temp == k:
                fitness += total
        return fitness

    # ── HUI insertion ────────────────────────

    def _insert(self, temp_particle: Particle):
        label = " ".join(
            str(self.twu_pattern[i])
            for i in range(len(self.twu_pattern))
            if temp_particle.X[i] == 1
        ) + " "

        for hui in self.hui_sets:
            if hui.itemset == label:
                return   # already exists
        self.hui_sets.append(HUI(label, temp_particle.fitness))

    # ── Output ───────────────────────────────

    def _write_out(self, output_path: str):
        lines = []
        for hui in self.hui_sets:
            lines.append(f"{hui.itemset}#UTIL: {hui.fitness}")
        with open(output_path, "w") as f:
            f.write("\n".join(lines))
            if lines:
                f.write("\n")

    # ── Memory check (approximate) ──────────

    def _check_memory(self):
        import tracemalloc
        if not tracemalloc.is_tracing():
            return
        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / 1024 / 1024
        if current_mb > self.max_memory:
            self.max_memory = current_mb

    # ── Stats ────────────────────────────────

    def print_stats(self):
        elapsed_ms = (self.end_timestamp - self.start_timestamp) * 1000
        print("=============  HUIM-BPSO-tree ALGORITHM v.2.11 - STATS =============")
        print(f" Total time ~ {elapsed_ms:.0f} ms")
        print(f" Memory ~ {self.max_memory:.4f} MB")
        print(f" High-utility itemsets count : {len(self.hui_sets)}")
        print("===================================================")


# ── Main ─────────────────────────────────────

if __name__ == "__main__":
    import tracemalloc
    tracemalloc.start()

    # Resolve input path relative to this script's location
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    input_path  = os.path.join(script_dir, INPUT_FILE)
    output_path = os.path.join(script_dir, OUTPUT_FILE)

    print(f"Running HUIM-BPSO-tree  |  min_utility = {MIN_UTILITY}")
    print(f"Input  : {input_path}")
    print(f"Output : {output_path}\n")

    algo = AlgoHUIM_BPSO_tree()
    algo.run_algorithm(input_path, output_path, MIN_UTILITY)
    algo.print_stats()

    tracemalloc.stop()

    # ── Show results ──────────────────────────
    print(f"\n{'─'*50}")
    print(f"High-Utility Itemsets found (min_utility={MIN_UTILITY}):")
    print(f"{'─'*50}")
    if algo.hui_sets:
        for hui in algo.hui_sets:
            print(f"  {hui.itemset.strip():<30}  utility = {hui.fitness}")
    else:
        print("  (none found)")
    print(f"{'─'*50}")
    print(f"\nResults also written to: {output_path}")