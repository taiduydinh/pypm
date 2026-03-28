import os
import time
from itertools import combinations

# ==========================================================
# AUTO PATH FIX
# ==========================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_FILE = os.path.join(CURRENT_DIR, "contextHAUIMMAU.txt")
MAU_FILE = os.path.join(CURRENT_DIR, "MAU_Utility.txt")
OUTPUT_FILE = os.path.join(CURRENT_DIR, "output.txt")

GLMAU = 0  # <<< CHANGE GLOBAL LEAST MAU HERE

# ==========================================================
# DATA STRUCTURES
# ==========================================================

class Transaction:
    def __init__(self, items, utilities):
        self.items = items
        self.utilities = utilities
        self.tu = max(utilities)

class Database:
    def __init__(self):
        self.transactions = []
        self.multiple_mau = {}

    def load(self, data_path, mau_path):
        # Load transactions
        with open(data_path, "r") as f:
            for line in f:
                if not line.strip() or line[0] in "#%@":
                    continue
                split = line.strip().split(":")
                items = list(map(int, split[0].split()))
                utils = list(map(int, split[2].split()))
                self.transactions.append(Transaction(items, utils))

        # Load multiple MAU
        with open(mau_path, "r") as f:
            for line in f:
                if not line.strip() or line[0] in "#%@":
                    continue
                item, mau = map(int, line.strip().split())
                self.multiple_mau[item] = mau

    def get_LMAU(self, glmau):
        lmau = min(self.multiple_mau.values())
        return max(lmau, glmau)

# ==========================================================
# ALGORITHM
# ==========================================================

class HAUIM_MMAU:

    def __init__(self, db):
        self.db = db
        self.high_itemsets = []
        self.start_time = 0
        self.end_time = 0

    def itemset_mau(self, itemset):
        total = 0
        for item in itemset:
            total += max(GLMAU, self.db.multiple_mau[item])
        return total / len(itemset)

    def run(self):
        self.start_time = time.time()

        LMAU = self.db.get_LMAU(GLMAU)

        # Phase 1: Generate candidates
        item_tid = {}
        item_auub = {}

        for tid, trans in enumerate(self.db.transactions):
            for item in trans.items:
                item_tid.setdefault(item, set()).add(tid)
                item_auub[item] = item_auub.get(item, 0) + trans.tu

        level = []

        for item in sorted(item_tid.keys()):
            if item_auub[item] >= LMAU:
                level.append([item])

        all_candidates = level.copy()

        k = 2
        while level:
            next_level = []
            for i in range(len(level)):
                for j in range(i+1, len(level)):
                    if level[i][:-1] == level[j][:-1]:
                        candidate = sorted(set(level[i]) | set(level[j]))

                        # compute tidset
                        tids = set.intersection(*[
                            item_tid[it] for it in candidate
                        ])

                        # AUUB
                        auub = sum(self.db.transactions[t].tu for t in tids)

                        if auub >= self.itemset_mau(candidate):
                            next_level.append(candidate)

            level = next_level
            all_candidates.extend(level)
            k += 1

        # Phase 2: Exact AU calculation
        results = []

        for candidate in all_candidates:
            total_util = 0

            for trans in self.db.transactions:
                if all(item in trans.items for item in candidate):
                    for item in candidate:
                        idx = trans.items.index(item)
                        total_util += trans.utilities[idx]

            avg_util = total_util // len(candidate)

            if avg_util >= self.itemset_mau(candidate):
                results.append((candidate, avg_util))

        self.high_itemsets = results
        self.end_time = time.time()

    def save(self):
    # Sort exactly like Java:
    # 1) by itemset length (level order)
    # 2) lexicographically within same length
        self.high_itemsets.sort(key=lambda x: (len(x[0]), x[0]))

        with open(OUTPUT_FILE, "w") as f:
            for items, util in self.high_itemsets:
                line = " ".join(map(str, items))
                f.write(f"{line} #AUTIL: {util}\n")
    def print_stats(self):
        print("============= HAUIM-MMAU PYTHON VERSION =============")
        print("Total time ~", int((self.end_time - self.start_time)*1000), "ms")
        print("High average-utility itemsets count:", len(self.high_itemsets))
        print("=====================================================")


# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    if not os.path.exists(DATA_FILE):
        print("Dataset not found.")
        print("Looking in:", CURRENT_DIR)
        print("Files:", os.listdir(CURRENT_DIR))
        exit()

    db = Database()
    db.load(DATA_FILE, MAU_FILE)

    miner = HAUIM_MMAU(db)
    miner.run()
    miner.save()
    miner.print_stats()