# =========================================
# HUIM-SU - High Utility Itemset Mining with Subtree Utility
# =========================================

import time
import os
from collections import defaultdict

# -------------------------
# MemoryLogger (dummy, compatible)
# -------------------------
class MemoryLogger:
    _instance = None

    def __init__(self):
        self.max_memory = 0

    @staticmethod
    def getInstance():
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def reset(self):
        self.max_memory = 0

    def checkMemory(self):
        return 0

    def getMaxMemory(self):
        return self.max_memory


# -------------------------
# HUIM-SU Algorithm
# -------------------------
class AlgoHUIMSU:

    def __init__(self):
        self.mapItemToTWU = {}
        self.mapALL = {}
        self.frontitemsdata = []
        self.itemsdata = []
        self.items = []
        self.newitems = []
        self.htwu = []
        self.ItemTWU = []
        self.temp = []
        self.transactionsnum = 0
        self.joinCount = 0
        self.huiCount = 0
        self.min_utility = 0

    # -------------------------
    # Run Algorithm
    # -------------------------
    def runAlgorithm(self, input_file, output_file, min_utility):
        MemoryLogger.getInstance().reset()
        self.min_utility = min_utility

        start = time.time()
        self.writer = open(output_file, "w")

        self.mapItemToTWU = {}
        self.mapALL = {}
        frontitems = defaultdict(set)

        ff = []
        maxitem = 0
        tid = 0

        # ---------- FIRST DB SCAN ----------
        with open(input_file) as f:
            for line in f:
                if not line.strip() or line[0] in "#%@":
                    continue

                parts = line.strip().split(":")
                items = list(map(int, parts[0].split()))
                values = list(map(int, parts[2].split()))
                tu = int(parts[1])

                self.htwu.append(tu)
                ff1 = []

                for i in range(len(items)):
                    item = items[i]
                    value = values[i]
                    ff1.append(item)

                    maxitem = max(maxitem, item)
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + tu

                    frontitems[item].update(items)

                    if item not in self.mapALL:
                        self.mapALL[item] = [tid, value]
                    else:
                        self.mapALL[item].extend([tid, value])

                ff.append(ff1)
                tid += 1

        self.transactionsnum = tid

        # ---------- ITERATIVE TWU PRUNING ----------
        self.ItemTWU = [0] * (maxitem + 1)
        active = []

        for k, v in self.mapItemToTWU.items():
            self.ItemTWU[k] = v
            active.append(k)

        changed = True
        while changed:
            changed = False
            i = 0
            while i < len(active):
                it = active[i]
                if self.ItemTWU[it] < self.min_utility:
                    data = self.mapALL[it]
                    for j in range(0, len(data), 2):
                        t = data[j]
                        val = data[j + 1]
                        for x in ff[t]:
                            self.ItemTWU[x] -= val
                    active.pop(i)
                    changed = True
                else:
                    i += 1

        active.sort(key=lambda x: self.ItemTWU[x])

        # ---------- PREPARE STRUCTURES ----------
        self.temp = [0] * len(active)

        for idx, item in enumerate(active):
            self.items.append(item)
            self.newitems.append(idx)
            self.itemsdata.append(self.mapALL[item])
            self.frontitemsdata.append(self.getfrontitem(frontitems[item], active, idx))

        jianzhiitems = self.firstjianzhi(self.newitems)
        self.speedminer(jianzhiitems)

        self.writer.close()
        end = time.time()

        print(f"Finished in {(end - start)*1000:.2f} ms")
        print("HUIs:", self.huiCount)
        print("Join count:", self.joinCount)

    # -------------------------
    # Pruning helpers
    # -------------------------
    def firstjianzhi(self, newitems):
        transactions = [0] * self.transactionsnum
        result = list(newitems)

        for i in range(len(result)-1, -1, -1):
            total = 0
            data = self.itemsdata[result[i]]
            for j in range(0, len(data), 2):
                transactions[data[j]] += data[j+1]
                total += transactions[data[j]]
            if total < self.min_utility:
                result.pop(i)
        return result

    def getfrontitem(self, itemset, active, k):
        return [i for i in range(k+1, len(active)) if active[i] in itemset]

    # -------------------------
    # Core mining
    # -------------------------
    def speedminer(self, jianzhiitems):
        self.joinCount += len(jianzhiitems)
        for idx in jianzhiitems:
            data = self.itemsdata[idx]
            total = sum(data[j] for j in range(1, len(data), 2))
            self.temp[0] = idx

            if total >= self.min_utility:
                self.writeOut(0, total)

            next_items = self.secondjianzhi(self.frontitemsdata[idx], data)
            self.construct(next_items, self.frontitemsdata[idx], data, 0)

    def construct(self, jianzhi, temp2, twu, prefixLength):
        prefixLength += 1
        self.joinCount += len(jianzhi)

        for i, idx in enumerate(jianzhi):
            self.temp[prefixLength] = idx
            afterdata = self.itemsdata[idx]

            total = 0
            currentdata = []
            j = k = 0

            while j < len(twu) and k < len(afterdata):
                if twu[j] == afterdata[k]:
                    total += twu[j+1] + afterdata[k+1]
                    currentdata.extend([twu[j], twu[j+1] + afterdata[k+1]])
                    j += 2
                    k += 2
                elif twu[j] > afterdata[k]:
                    k += 2
                else:
                    j += 2

            if total >= self.min_utility:
                self.writeOut(prefixLength, total)

            afteritem = self.frontitemsdata[idx]
            next_items = []
            a = i+1
            b = 0
            while a < len(temp2) and b < len(afteritem):
                if temp2[a] == afteritem[b]:
                    next_items.append(temp2[a])
                    a += 1
                    b += 1
                elif temp2[a] > afteritem[b]:
                    b += 1
                else:
                    a += 1

            next_jianzhi = self.secondjianzhi(next_items, currentdata)
            self.construct(next_jianzhi, next_items, currentdata, prefixLength)

    def secondjianzhi(self, temp2, twu):
        result = list(temp2)
        transactions = [0] * self.transactionsnum

        for j in range(0, len(twu), 2):
            transactions[twu[j]] = twu[j+1]

        for i in range(len(result)-1, -1, -1):
            data = self.itemsdata[result[i]]
            total = 0
            j = k = 0

            while j < len(twu) and k < len(data):
                if twu[j] == data[k]:
                    transactions[data[k]] += data[k+1]
                    total += transactions[data[k]]
                    j += 2
                    k += 2
                elif twu[j] > data[k]:
                    k += 2
                else:
                    j += 2

            if total < self.min_utility:
                result.pop(i)

        return result

    # -------------------------
    # Output
    # -------------------------
    def writeOut(self, prefixLength, total):
        self.huiCount += 1
        out = []
        for i in range(prefixLength + 1):
            out.append(str(self.items[self.temp[i]]))
        self.writer.write(" ".join(out) + f" #UTIL: {total}\n")


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    BASE = os.path.dirname(os.path.abspath(__file__))
    INPUT = os.path.join(BASE, "DB_Utility.txt")
    OUTPUT = os.path.join(BASE, "output.txt")
    MIN_UTILITY = 30

    algo = AlgoHUIMSU()
    algo.runAlgorithm(INPUT, OUTPUT, MIN_UTILITY)
