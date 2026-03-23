import os
import time
from collections import defaultdict

# =====================================================
# AUTO PATH FIX
# =====================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_DIR = CURRENT_DIR

PROFIT_FILE = os.path.join(BASE_DIR, "UtilityDB_profit.txt")
DB_FILE = os.path.join(BASE_DIR, "UtilityDB.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "#111_output.txt")

# PARAMETERS (same as Java)
BETA = 2
GLMAU = 30


# =====================================================
# DATA STRUCTURES
# =====================================================

class CAUEntry:
    def __init__(self, tid, utility, rmu, remu):
        self.tid = tid
        self.utility = utility
        self.rmu = rmu
        self.remu = remu


class CAUList:
    def __init__(self, item):
        self.item = item
        self.sumUtility = 0
        self.sumOfRmu = 0
        self.sumOfRemu = 0
        self.entries = []

    def add(self, entry):
        self.sumUtility += entry.utility
        self.sumOfRmu += entry.rmu
        self.sumOfRemu += entry.remu
        self.entries.append(entry)


# =====================================================
# MEMU ALGORITHM
# =====================================================

class MEMU:

    def __init__(self):
        self.item2mau = {}
        self.leastMAU = 0
        self.mapEUCS = defaultdict(dict)
        self.hauiCount = 0
        self.candidateCount = 0
        self.start = 0
        self.end = 0
        self.writer = None

    # -------------------------------------------------

    def read_profits(self, path):
        profits = {}
        with open(path, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                item, profit = line.strip().split(",")
                profits[int(item)] = int(profit)
        return profits

    # -------------------------------------------------

    def compare_items(self, i1, i2):
        diff = self.item2mau[i1] - self.item2mau[i2]
        if diff == 0:
            return i1 - i2
        return diff

    # -------------------------------------------------

    def run(self):

        self.start = time.time()

        profits = self.read_profits(PROFIT_FILE)

        # Generate MAU per item
        self.leastMAU = float("inf")
        for item, profit in profits.items():
            val = max(profit * BETA, GLMAU)
            self.item2mau[item] = val
            self.leastMAU = min(self.leastMAU, val)

        # -------------------------------------------------
        # FIRST SCAN: AUUB
        # -------------------------------------------------

        item2auub = defaultdict(int)

        with open(DB_FILE, "r") as f:
            for line in f:
                parts = line.strip().split()
                maxUtility = 0

                for i in range(0, len(parts), 2):
                    item = int(parts[i])
                    qty = int(parts[i + 1])
                    util = qty * profits[item]
                    maxUtility = max(maxUtility, util)

                for i in range(0, len(parts), 2):
                    item = int(parts[i])
                    item2auub[item] += maxUtility

        # -------------------------------------------------
        # CREATE CAULISTS
        # -------------------------------------------------

        mapItemToList = {}
        listOfLists = []

        for item, auub in item2auub.items():
            if auub >= self.leastMAU:
                ul = CAUList(item)
                mapItemToList[item] = ul
                listOfLists.append(ul)

        # sort by MAU ascending
        listOfLists.sort(key=lambda x: (self.item2mau[x.item], x.item))

        # -------------------------------------------------
        # SECOND SCAN: BUILD CAULISTS
        # -------------------------------------------------

        with open(DB_FILE, "r") as f:
            tid = 0
            for line in f:
                parts = line.strip().split()
                revised = []
                maxUtility = 0

                for i in range(0, len(parts), 2):
                    item = int(parts[i])
                    qty = int(parts[i + 1])
                    util = qty * profits[item]

                    if item2auub[item] >= self.leastMAU:
                        revised.append((item, util))
                        maxUtility = max(maxUtility, util)

                revised.sort(key=lambda x: (self.item2mau[x[0]], x[0]))

                rmu = 0
                remu = 0

                for item, util in reversed(revised):
                    rmu = max(rmu, util)
                    entry = CAUEntry(tid, util, rmu, remu)
                    mapItemToList[item].add(entry)
                    remu = max(remu, util)

                # EUCS
                for i in range(len(revised)):
                    item_i, _ = revised[i]
                    for j in range(i + 1, len(revised)):
                        item_j, _ = revised[j]
                        self.mapEUCS[item_i][item_j] = \
                            self.mapEUCS[item_i].get(item_j, 0) + maxUtility

                tid += 1

        self.writer = open(OUTPUT_FILE, "w")

        self.search([], None, listOfLists, 0)

        self.writer.close()

        self.end = time.time()

    # -------------------------------------------------

    def search(self, prefix, p, lists, sumMAU):

        for i in range(len(lists)):

            x = lists[i]
            sumMAUPx = sumMAU + self.item2mau[x.item]

            # output
            if x.sumUtility >= sumMAUPx:
                self.hauiCount += 1
                avgUtil = x.sumUtility / (len(prefix) + 1)
                mauVal = sumMAUPx / (len(prefix) + 1)
                self.write(prefix, x.item, avgUtil, mauVal)

            # loose bound
            if (x.sumUtility + x.sumOfRemu * (len(prefix) + 1)) < sumMAUPx:
                continue

            # tight bound
            if x.sumOfRmu * (len(prefix) + 1) >= sumMAUPx:

                exLists = []

                for j in range(i + 1, len(lists)):
                    y = lists[j]

                    # EUCS pruning
                    if x.item in self.mapEUCS:
                        if y.item in self.mapEUCS[x.item]:
                            if self.mapEUCS[x.item][y.item] < self.leastMAU:
                                continue

                    newList = self.construct(prefix, p, x, y, sumMAUPx)
                    if newList:
                        exLists.append(newList)

                self.search(prefix + [x.item], x, exLists, sumMAUPx)

    # -------------------------------------------------

    def construct(self, prefix, p, px, py, sumMAUPx):

        newList = CAUList(py.item)

        for ex in px.entries:

            ey = next((e for e in py.entries if e.tid == ex.tid), None)
            if not ey:
                continue

            if p is None:
                util = ex.utility + ey.utility
            else:
                pe = next((e for e in p.entries if e.tid == ex.tid), None)
                if not pe:
                    continue
                util = ex.utility + ey.utility - pe.utility

            entry = CAUEntry(ex.tid, util, ex.rmu, ey.remu)
            newList.add(entry)

        if newList.sumUtility == 0:
            return None

        return newList

    # -------------------------------------------------

    def write(self, prefix, item, avg, mau):

        line = ""
        for p in prefix:
            line += str(p) + " "
        line += str(item)
        line += " #AUTIL: {:.2f} #mau: {:.2f}".format(avg, mau)
        self.writer.write(line + "\n")


# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":
    miner = MEMU()
    miner.run()

    print("=============  MEMU PYTHON  =============")
    print("Total time ~ {:.2f} ms".format((miner.end - miner.start) * 1000))
    print("High-utility itemsets count :", miner.hauiCount)
    print("==========================================")