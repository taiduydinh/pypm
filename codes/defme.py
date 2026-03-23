# ============================================================
# DefMe Algorithm — Python Version
# ============================================================

import time
import os
import math


# ============================================================
# Memory Logger
# ============================================================
class MemoryLogger:
    _instance = None

    def __init__(self):
        self.maxMemory = 0.0

    @staticmethod
    def getInstance():
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def getMaxMemory(self):
        return self.maxMemory

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self):
        current = (os.getpid() % 1024) + 0.001  # dummy small check (no psutil)
        if current > self.maxMemory:
            self.maxMemory = current
        return current


# ============================================================
# Itemset & Itemsets
# ============================================================
class Itemset:
    def __init__(self, items, bitset=None, support=0):
        self.itemset = items
        self.transactionsIds = bitset
        self.cardinality = support

    def getAbsoluteSupport(self):
        return self.cardinality

    def getItems(self):
        return self.itemset

    def size(self):
        return len(self.itemset)

    def setTIDs(self, bitset, cardinality):
        self.transactionsIds = bitset
        self.cardinality = cardinality


class Itemsets:
    def __init__(self, name):
        self.name = name
        self.levels = [[]]
        self.itemsetsCount = 0

    def addItemset(self, itemset, k):
        while len(self.levels) <= k:
            self.levels.append([])
        self.levels[k].append(itemset)
        self.itemsetsCount += 1

    def getLevels(self):
        return self.levels

    def getItemsetsCount(self):
        return self.itemsetsCount


# ============================================================
# Transaction Database
# ============================================================
class TransactionDatabase:
    def __init__(self):
        self.items = set()
        self.transactions = []

    def loadFile(self, path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith(("#", "%", "@")):
                    parts = [int(x) for x in line.split()]
                    self.addTransaction(parts)

    def addTransaction(self, transaction):
        self.transactions.append(transaction)
        self.items.update(transaction)

    def size(self):
        return len(self.transactions)

    def getTransactions(self):
        return self.transactions


# ============================================================
# AlgoDefMe — main algorithm implementation
# ============================================================
class AlgoDefMe:
    def __init__(self):
        self.minsupRelative = 0
        self.database = None
        self.startTimestamp = 0
        self.endTime = 0
        self.generators = None
        self.writer = None
        self.itemsetCount = 0
        self.mapItemTIDS = {}
        self.itemsetBuffer = []
        self.maxItemsetSize = float("inf")

    def runAlgorithm(self, output, database, minsup):
        MemoryLogger.getInstance().reset()
        self.itemsetBuffer = [0] * 2000
        self.database = database
        self.startTimestamp = time.time()

        # ✅ Exact same rounding as Java's Math.ceil()
        self.minsupRelative = math.ceil(minsup * database.size())
        print(f"[DEBUG] minsupRelative = {self.minsupRelative} of {database.size()} transactions")

        if output:
            self.writer = open(output, "w")
            self.generators = None
        else:
            self.writer = None
            self.generators = Itemsets("FREQUENT ITEMSETS")

        self.itemsetCount = 0

        # Build map of item → transaction IDs
        self.mapItemTIDS = {}
        for tid, transaction in enumerate(database.getTransactions()):
            for item in transaction:
                if item not in self.mapItemTIDS:
                    self.mapItemTIDS[item] = {"bitset": set(), "support": 0}
                self.mapItemTIDS[item]["bitset"].add(tid)
                self.mapItemTIDS[item]["support"] += 1

        # Get frequent items
        frequentItems = [item for item, data in self.mapItemTIDS.items()
                         if data["support"] >= self.minsupRelative and self.maxItemsetSize >= 1]

        # Sort by increasing support, tie-breaker by item id to match Java ordering
        frequentItems.sort(key=lambda i: (self.mapItemTIDS[i]["support"], i))

        # Empty set
        tidsetEmpty = set(range(database.size()))
        self.defme([], tidsetEmpty, database.size(), frequentItems, 0, [])

        MemoryLogger.getInstance().checkMemory()

        if self.writer:
            self.writer.close()
        self.endTime = time.time()

        return self.generators

    def defme(self, itemsetX, tidsetX, supportX, frequentItems, posTail, critItemsetX):
        if itemsetX:
            for covStarXe in critItemsetX:
                if not covStarXe:
                    return

        self.save(itemsetX, tidsetX, supportX)

        if len(itemsetX) < self.maxItemsetSize:
            for i in range(posTail, len(frequentItems)):
                e = frequentItems[i]
                tidsetE = self.mapItemTIDS[e]["bitset"]
                itemsetXe = itemsetX + [e]
                tidsetXe = tidsetX & tidsetE
                supportXe = len(tidsetXe)
                if supportXe < self.minsupRelative:
                    continue

                critItemsetY = []
                critE = tidsetX - tidsetE
                critItemsetY.append(critE)

                for j in range(len(itemsetX)):
                    critItemsetY.append(critItemsetX[j] & tidsetE)

                self.defme(itemsetXe, tidsetXe, supportXe, frequentItems, i + 1, critItemsetY)

    def save(self, prefix, tidset, support):
        self.itemsetCount += 1
        if self.writer:
            if prefix:
                buffer = " ".join(map(str, prefix)) + f" #SUP: {support}\n"
            else:
                buffer = f"#SUP: {support}\n"
            self.writer.write(buffer)
        else:
            itemset = Itemset(prefix)
            itemset.setTIDs(tidset, support)
            self.generators.addItemset(itemset, len(prefix))

    def printStats(self):
        print("=============  DefMe - STATS =============")
        print(f" Transactions count from database : {self.database.size()}")
        print(f" Generator itemsets count : {self.itemsetCount}")
        print(f" Total time ~ {round((self.endTime - self.startTimestamp) * 1000, 2)} ms")
        print(f" Maximum memory usage : {MemoryLogger.getInstance().getMaxMemory()} mb")
        print("=========================================")

    def setMaximumPatternLength(self, length):
        self.maxItemsetSize = length


# ============================================================
# MAIN EXECUTION (Equivalent to MainTestDefMe_saveToFile.java)
# ============================================================
if __name__ == "__main__":
    print("============= DEFME ALGORITHM =============")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data = os.path.join(script_dir, "contextZart.txt")
    minsup = 0.2              # adjust freely for testing
    mode = "file"             # "memory" or "file"
    output = os.path.join(script_dir, "defme_outputs.txt")
    maxlen = None             # or set integer limit (e.g., 3)

    db = TransactionDatabase()
    db.loadFile(data)

    algo = AlgoDefMe()
    if maxlen:
        algo.setMaximumPatternLength(maxlen)

    gens = algo.runAlgorithm(output if mode == "file" else None, db, minsup)
    algo.printStats()

    if mode == "memory":
        for level in gens.getLevels():
            for itemset in level:
                print(itemset.getItems(), "#SUP:", itemset.getAbsoluteSupport())

    print("\nMining complete! Output saved to:", output)
    print("=========================================")
