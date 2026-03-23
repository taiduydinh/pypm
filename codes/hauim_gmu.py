import os
import time
from itertools import combinations

# ==========================================================
# AUTO PATH FIX (WORKS FROM Java OR Java/src)
# ==========================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

if os.path.basename(CURRENT_DIR) == "src":
    BASE_DIR = os.path.dirname(CURRENT_DIR)
    SRC_DIR = CURRENT_DIR
else:
    BASE_DIR = CURRENT_DIR
    SRC_DIR = os.path.join(BASE_DIR, "src")

INPUT_FILE = os.path.join(SRC_DIR, "contextHAUIMiner.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "#109_output.txt")

MAU = 30   # <<< CHANGE THRESHOLD HERE


# ==========================================================
# LOAD DATABASE
# ==========================================================

def load_database(path):
    database = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] in "#%@":
                continue

            parts = line.split(":")
            items = list(map(int, parts[0].split()))
            utils = list(map(int, parts[2].split()))

            transaction = {}
            for i in range(len(items)):
                transaction[items[i]] = utils[i]

            database.append(transaction)

    return database


# ==========================================================
# HAUIM-GMU IMPLEMENTATION
# ==========================================================

class HAUIM_GMU:

    def __init__(self):
        self.database = []
        self.mau = 0
        self.results = []
        self.start = 0
        self.end = 0

    def run_algorithm(self, input_path, output_path, threshold):

        self.start = time.time()
        self.database = load_database(input_path)
        self.mau = threshold

        # ----------------------------
        # Step 1: Compute AUUB
        # ----------------------------
        mapItemToAUUB = {}

        for transaction in self.database:
            max_utility = max(transaction.values())
            for item in transaction:
                mapItemToAUUB[item] = mapItemToAUUB.get(item, 0) + max_utility

        # Keep promising items
        promising_items = sorted(
            [item for item in mapItemToAUUB if mapItemToAUUB[item] >= self.mau]
        )

        # ----------------------------
        # Step 2: Build TID sets
        # ----------------------------
        mapItemToTid = {}
        mapItemToTidUtility = {}

        for tid, transaction in enumerate(self.database):
            for item in promising_items:
                if item in transaction:
                    mapItemToTid.setdefault(item, set()).add(tid)
                    mapItemToTidUtility.setdefault(item, {})[tid] = transaction[item]

        # ----------------------------
        # Step 3: Generate itemsets
        # ----------------------------
        for r in range(1, len(promising_items) + 1):
            for comb in combinations(promising_items, r):

                tidset = set.intersection(*(mapItemToTid[i] for i in comb))

                if not tidset:
                    continue

                total_utility = 0
                for tid in tidset:
                    for item in comb:
                        total_utility += mapItemToTidUtility[item][tid]

                avg_utility = total_utility // len(comb)

                if avg_utility >= self.mau:
                    self.results.append((comb, avg_utility))

        # ----------------------------
        # SORT LIKE JAVA
        # ----------------------------
        self.results.sort(key=lambda x: (len(x[0]), x[0]))

        # Write output
        self.write_output(output_path)

        self.end = time.time()

    # ------------------------------------------------------

    def write_output(self, path):
        with open(path, "w") as f:
            for itemset, autil in self.results:

                # Remove .0 if integer (match Java)
                if autil == int(autil):
                    autil_str = str(int(autil))
                else:
                    autil_str = str(autil)

                line = " ".join(map(str, itemset))
                f.write(f"{line} #AUTIL: {autil_str}\n")

    # ------------------------------------------------------

    def print_stats(self):
        print("============= HAUIM-GMU (Python) =============")
        print(f"Total time ~ {(self.end - self.start)*1000:.2f} ms")
        print(f"HAUI count: {len(self.results)}")
        print("==============================================")


# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    if not os.path.exists(INPUT_FILE):
        print("Dataset not found.")
        print("Looking inside:", SRC_DIR)
        print("Files:", os.listdir(SRC_DIR))
        exit()

    miner = HAUIM_GMU()
    miner.run_algorithm(INPUT_FILE, OUTPUT_FILE, MAU)
    miner.print_stats()

    print("\nOutput written to:", OUTPUT_FILE)