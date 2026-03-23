"""
HUIF-PSO: High Utility Itemset Mining using Particle Swarm Optimization
Python conversion of AlgoBio_HUIF_PSO.java

Reference:
    Wei Song, Chaomin Huang. Mining High Utility Itemsets Using Bio-Inspired Algorithms:
    A Diverse Optimal Value Framework. IEEE Access, 2018, 6(1): 19568-19582.

Usage:
    1. Press the "Play" button in VS Code or run: python Algobio_huif_pso.py
    2. Adjust MIN_UTILITY below to test different thresholds
    3. Set RANDOM_SEED to a fixed number for reproducible results
    4. The program automatically compares with Java output if available
    
Testing with Java:
    - Change MIN_UTILITY in both Java and Python to the same value
    - Run both versions
    - The Python version will automatically verify if outputs match
    - Both should find the same high-utility itemsets (order may vary)
    
Output:
    - Writes results to Python.output.txt in the same folder
    - Prints all discovered itemsets to console
    - Shows comparison with Java.output.txt if it exists
"""

import time
import random
import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple


# ─────────────────────────────────────────────
#  CONFIGURATION  ← change these to test
# ─────────────────────────────────────────────
INPUT_FILE  = "contextHUIM.txt"   # must be in the same folder as this script
OUTPUT_FILE = "Python.output.txt"
MIN_UTILITY = 35                   # CHANGED: testing with MIN_UTILITY = 30

POP_SIZE  = 100    # population size  (matches Java: pop_size  = 100)
MAX_ITER  = 2000   # max iterations   (matches Java: max_iter  = 2000)

# Set random seed for reproducible results (set to None for different results each run)
# Use the same seed to get identical results across multiple runs
RANDOM_SEED = 42   # change this number for different but reproducible results

# Sort output for easier comparison between Java and Python
SORT_OUTPUT = True  # True = sorted by itemset size then lexicographically
# ─────────────────────────────────────────────


# ── data structures ──────────────────────────

@dataclass
class Pair:
    """Item + its utility inside one transaction."""
    item: int = 0
    utility: int = 0


@dataclass
class HUI:
    """A discovered High-Utility Itemset."""
    itemset: str = ""
    fitness: int = 0


# ── global state (mirrors Java instance fields) ──

map_item_to_twu:  Dict[int, int] = {}   # TWU of items that pass the threshold
map_item_to_twu0: Dict[int, int] = {}   # TWU of all items (used for filtering)
twu_pattern:      List[int]      = []   # sorted list of promising items
database:         List[List[Pair]] = [] # transactions (only promising items)
transaction_count: int            = 0

# Bitmap representation: Items[j].TIDS is a set of transaction indices
class Item:
    def __init__(self, item: int):
        self.item = item
        self.TIDS: set = set()   # set of transaction indices where item appears


items_list: List[Item] = []   # mirrors Java List<Item> Items

hui_sets:   List[HUI]      = []   # discovered HUIs
hui_ba:     List["Particle"] = []  # unique HUI particles (for gBest reinit)
percentage: List[float]    = []   # roulette weights from TWU

population: List["Particle"] = []
p_best:     List["Particle"] = []
g_best:     Optional["Particle"] = None


# ── Particle ──────────────────────────────────

class Particle:
    """
    Mirrors the Java Particle inner class.
    X is a Python list of booleans (index = position in twu_pattern).
    """

    def __init__(self, length: Optional[int] = None):
        size = length if length is not None else len(twu_pattern)
        self.X: List[bool] = [False] * size
        self.fitness: int = 0

    def copy_from(self, other: "Particle"):
        self.X = other.X[:]
        self.fitness = other.fitness

    # ── fitness calculation (mirrors calculateFitness) ──
    def calculate_fitness(self, k: int, trans_list: List[int]):
        """
        k          – number of set bits (cardinality)
        trans_list – transaction indices where the itemset is present
        """
        if k == 0:
            return

        fitness = 0
        for p in trans_list:
            i = 0   # pointer into X (bit positions)
            q = 0   # pointer into transaction pairs
            temp = 0
            total = 0
            tx = database[p]
            while q < len(tx) and i < len(self.X):
                if self.X[i]:
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


# ── helper functions ──────────────────────────

def pev_check(particle: Particle, result_list: List[int]) -> bool:
    """
    Mirrors Java pev_Check.
    Removes items from the particle that would make the itemset empty,
    then fills result_list with the transaction indices where the itemset occurs.
    Returns False if the resulting itemset is empty.
    """
    set_positions = [i for i, v in enumerate(particle.X) if v]
    if not set_positions:
        return False

    # start with the TID-set of the first item
    current_tids = items_list[set_positions[0]].TIDS.copy()
    mid_tids = current_tids.copy()

    for i in range(1, len(set_positions)):
        pos = set_positions[i]
        candidate = current_tids & items_list[pos].TIDS
        if candidate:
            current_tids = candidate
            mid_tids = candidate.copy()
        else:
            # this item makes the intersection empty → remove it from particle
            current_tids = mid_tids.copy()
            particle.X[pos] = False

    if not current_tids:
        return False

    result_list.extend(sorted(current_tids))
    return True


def bit_diff(a: Particle, b: Particle) -> List[int]:
    """XOR of two particles → positions that differ (mirrors Java bitDiff)."""
    return [i for i in range(len(a.X)) if a.X[i] != b.X[i]]


def add_hui_ba(particle: Particle):
    """
    Add particle to huiBA if it is not already present (mirrors addHuiBA).
    """
    for existing in hui_ba:
        if existing.X == particle.X:
            return
    new_p = Particle(len(twu_pattern))
    new_p.copy_from(particle)
    hui_ba.append(new_p)


def insert(particle: Particle):
    """
    Convert particle bits to an itemset string and add to hui_sets if new
    (mirrors Java insert).
    """
    parts = []
    for i, v in enumerate(particle.X):
        if v:
            parts.append(str(twu_pattern[i]))
    if not parts:
        return
    itemset_str = " ".join(parts) + " "   # Java appends a trailing space

    for h in hui_sets:
        if h.itemset == itemset_str:
            return
    hui_sets.append(HUI(itemset=itemset_str, fitness=particle.fitness))


# ── roulette selection ────────────────────────

def roulette_percent() -> List[float]:
    """Build cumulative TWU-proportion list (mirrors roulettePercent)."""
    total = sum(map_item_to_twu[item] for item in twu_pattern)
    cumulative = []
    running = 0.0
    for item in twu_pattern:
        running += map_item_to_twu[item]
        cumulative.append(running / total)
    return cumulative


def roulette_select(percents: List[float]) -> int:
    """Select an index using roulette wheel (mirrors rouletteSelect)."""
    r = random.random()
    for i, p in enumerate(percents):
        if i == 0:
            if 0 <= r <= p:
                return 0
        elif percents[i - 1] < r <= p:
            return i
    return len(percents) - 1


def roulette_percent_hui_ba() -> List[float]:
    """Build cumulative fitness-proportion list for huiBA (mirrors roulettePercentHUIBA)."""
    total = sum(p.fitness for p in hui_ba)
    cumulative = []
    running = 0.0
    for p in hui_ba:
        running += p.fitness
        cumulative.append(running / total)
    return cumulative


def roulette_select_hui_ba(percents: List[float]) -> int:
    """Select index from huiBA by roulette (mirrors rouletteSelectHUIBA)."""
    r = random.random()
    for i, p in enumerate(percents):
        if i == 0:
            if 0 <= r <= p:
                return 0
        elif percents[i - 1] < r <= p:
            return i
    return len(percents) - 1


# ── population initialisation ─────────────────

def pop_init(min_utility: int):
    """Mirrors Java pop_Init."""
    global g_best

    percents = roulette_percent()
    print("percentage:", percents)

    for i in range(POP_SIZE):
        p = Particle(len(twu_pattern))
        k = int(random.random() * len(twu_pattern))
        j = 0
        while j < k:
            temp = roulette_select(percents)
            if not p.X[temp]:
                p.X[temp] = True
                j += 1

        trans_list: List[int] = []
        pev_check(p, trans_list)
        p.calculate_fitness(k, trans_list)

        population.append(p)
        p_best[i].copy_from(p)

        if p.fitness >= min_utility:
            insert(p)
            add_hui_ba(p)

        if i == 0:
            g_best.copy_from(p_best[i])
        else:
            if p_best[i].fitness > g_best.fitness:
                g_best.copy_from(p_best[i])


# ── generation update ─────────────────────────

def next_gen_pa(min_utility: int):
    """Mirrors Java next_Gen_PA (one full PSO iteration)."""
    global g_best

    for i in range(POP_SIZE):
        # move toward personal best
        dis_list = bit_diff(p_best[i], population[i])
        num = int(len(dis_list) * random.random()) + 1
        if dis_list:
            for _ in range(num):
                change_bit = dis_list[int(len(dis_list) * random.random())]
                population[i].X[change_bit] = not population[i].X[change_bit]

        # move toward global best
        dis_list = bit_diff(g_best, population[i])
        num = int(len(dis_list) * random.random()) + 1
        if dis_list:
            for _ in range(num):
                change_bit = dis_list[int(len(dis_list) * random.random())]
                population[i].X[change_bit] = not population[i].X[change_bit]

        # random flip (inertia / exploration)
        change_bit = int(len(twu_pattern) * random.random())
        population[i].X[change_bit] = not population[i].X[change_bit]

        k = sum(population[i].X)
        trans_list: List[int] = []
        pev_check(population[i], trans_list)
        population[i].calculate_fitness(k, trans_list)

        # update personal best and global best
        if population[i].fitness > p_best[i].fitness:
            p_best[i].copy_from(population[i])
            if p_best[i].fitness > g_best.fitness:
                g_best.copy_from(p_best[i])

        # update HUI sets
        if population[i].fitness >= min_utility:
            insert(population[i])
            add_hui_ba(population[i])


# ── main algorithm ────────────────────────────

def run_algorithm(input_path: str, output_path: str, min_utility: int):
    """
    Entry point – mirrors Java runAlgorithm.
    Reads the database, runs PSO, writes results.
    """
    global map_item_to_twu, map_item_to_twu0, twu_pattern
    global database, transaction_count, items_list
    global hui_sets, hui_ba, percentage, population, p_best, g_best

    # ── reset state ──
    map_item_to_twu  = {}
    map_item_to_twu0 = {}
    twu_pattern      = []
    database         = []
    transaction_count = 0
    items_list       = []
    hui_sets         = []
    hui_ba           = []
    percentage       = []
    population       = []
    p_best           = []

    # ── set random seed for reproducibility ──
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)
        print(f"Random seed set to: {RANDOM_SEED}")

    start_time = time.time()

    # ────────────────────────────────────────────
    # PASS 1 – compute TWU for every item
    # ────────────────────────────────────────────
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] in ('#', '%', '@'):
                continue
            transaction_count += 1
            parts = line.split(":")
            item_ids = parts[0].strip().split()
            tx_utility = int(parts[1].strip())
            for item_str in item_ids:
                item = int(item_str)
                map_item_to_twu[item]  = map_item_to_twu.get(item, 0) + tx_utility
                map_item_to_twu0[item] = map_item_to_twu0.get(item, 0) + tx_utility

    # ────────────────────────────────────────────
    # PASS 2 – build the filtered database
    # ────────────────────────────────────────────
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] in ('#', '%', '@'):
                continue
            parts = line.split(":")
            item_ids      = parts[0].strip().split()
            utility_vals  = parts[2].strip().split()
            revised_tx: List[Pair] = []
            for item_str, util_str in zip(item_ids, utility_vals):
                item = int(item_str)
                util = int(util_str)
                if map_item_to_twu.get(item, 0) >= min_utility:
                    revised_tx.append(Pair(item=item, utility=util))
                else:
                    map_item_to_twu0.pop(item, None)
            database.append(revised_tx)

    # promising items, sorted ascending (mirrors Collections.sort)
    twu_pattern = sorted(map_item_to_twu0.keys())
    print(f"twuPattern: {len(twu_pattern)}")
    print(twu_pattern)

    # ── build bitmap (TID sets) ──
    for item in twu_pattern:
        items_list.append(Item(item))

    for tx_idx, tx in enumerate(database):
        for j, it in enumerate(items_list):
            for pair in tx:
                if it.item == pair.item:
                    it.TIDS.add(tx_idx)

    # ── initialise pBest and gBest shells ──
    for _ in range(POP_SIZE):
        p_best.append(Particle(len(twu_pattern)))
    g_best = Particle(len(twu_pattern))

    # ── run PSO ──
    if twu_pattern:
        pop_init(min_utility)
        for iteration in range(MAX_ITER):
            next_gen_pa(min_utility)
            if hui_ba:
                pct_hui_ba = roulette_percent_hui_ba()
                num = roulette_select_hui_ba(pct_hui_ba)
                g_best.copy_from(hui_ba[num])
            if iteration % 200 == 0:
                print(f"{iteration}-update end. HUIs No. is {len(hui_sets)}")

    # ── write output ──
    with open(output_path, "w") as out:
        lines = []
        hui_list = hui_sets if not SORT_OUTPUT else sorted(hui_sets, key=lambda h: (len(h.itemset.split()), h.itemset))
        for h in hui_list:
            lines.append(f"{h.itemset}#UTIL: {h.fitness}")
        out.write("\n".join(lines))

    end_time = time.time()
    elapsed_ms = int((end_time - start_time) * 1000)

    return elapsed_ms


# ── stats printer ────────────────────────────

def print_stats(elapsed_ms: int):
    """Mirrors Java printStats."""
    print("=============  HUIF-PSO ALGORITHM v.2.11 - STATS =============")
    print(f" Total time ~ {elapsed_ms} ms")
    print(f" High-utility itemsets count : {len(hui_sets)}")
    print("===================================================")


# ── compare outputs ───────────────────────────

def compare_outputs(file1: str, file2: str) -> bool:
    """
    Compare two output files to verify they contain the same itemsets.
    Returns True if they match, False otherwise.
    """
    def parse_output_file(filepath):
        """Parse output file and return set of (itemset, utility) tuples."""
        itemsets = set()
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and "#UTIL:" in line:
                        parts = line.split("#UTIL:")
                        itemset = parts[0].strip()
                        utility = int(parts[1].strip())
                        itemsets.add((itemset, utility))
        except FileNotFoundError:
            return None
        return itemsets
    
    set1 = parse_output_file(file1)
    set2 = parse_output_file(file2)
    
    if set1 is None:
        print(f"Error: Could not read {file1}")
        return False
    if set2 is None:
        print(f"Error: Could not read {file2}")
        return False
    
    if set1 == set2:
        print(f"\n✓ SUCCESS: Outputs match perfectly!")
        print(f"  Both files contain {len(set1)} identical itemsets.")
        return True
    else:
        print(f"\n✗ MISMATCH: Outputs differ!")
        print(f"  File 1: {len(set1)} itemsets")
        print(f"  File 2: {len(set2)} itemsets")
        
        only_in_1 = set1 - set2
        only_in_2 = set2 - set1
        
        if only_in_1:
            print(f"\n  Only in {os.path.basename(file1)}:")
            for itemset, util in sorted(only_in_1):
                print(f"    {itemset} #UTIL: {util}")
        
        if only_in_2:
            print(f"\n  Only in {os.path.basename(file2)}:")
            for itemset, util in sorted(only_in_2):
                print(f"    {itemset} #UTIL: {util}")
        
        return False


# ── display results ───────────────────────────

def display_results():
    """Print every discovered HUI to the console."""
    print("\n─── Discovered High-Utility Itemsets ───")
    if not hui_sets:
        print("  (none found)")
    else:
        hui_list = hui_sets if not SORT_OUTPUT else sorted(hui_sets, key=lambda h: (len(h.itemset.split()), h.itemset))
        for h in hui_list:
            print(f"  {h.itemset.strip()}  #UTIL: {h.fitness}")
    print(f"\nTotal HUIs found: {len(hui_sets)}")


# ── entry point ───────────────────────────────

if __name__ == "__main__":
    # Resolve paths relative to this script's directory so VS Code
    # "Run" button works regardless of working directory.
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    input_path  = os.path.join(script_dir, INPUT_FILE)
    output_path = os.path.join(script_dir, OUTPUT_FILE)

    print(f"Input file  : {input_path}")
    print(f"Output file : {output_path}")
    print(f"Min utility : {MIN_UTILITY}")
    print()

    elapsed = run_algorithm(input_path, output_path, MIN_UTILITY)
    print_stats(elapsed)
    display_results()

    print(f"\nFull results also saved to: {output_path}")
    
    # ── Compare with Java output if available ──
    java_output = os.path.join(os.path.dirname(script_dir), "Java.output.txt")
    if os.path.exists(java_output):
        print(f"\n{'='*60}")
        print("Comparing Python output with Java output...")
        print(f"{'='*60}")
        compare_outputs(output_path, java_output)