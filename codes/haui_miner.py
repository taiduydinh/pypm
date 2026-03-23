import os
import time

# ==============================
# CONFIG
# ==============================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "src", "contextHAUIMiner.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "#107_output.txt")

MIN_AUTILITY = 30  # <<< CHANGE THRESHOLD HERE


# ==============================
# DATA STRUCTURES
# ==============================

class Element:
    def __init__(self, tid, iutils, mutils):
        self.tid = tid
        self.iutils = iutils
        self.mutils = mutils


class UtilityList:
    def __init__(self, item):
        self.item = item
        self.sumIutils = 0
        self.sumMutils = 0
        self.elements = []

    def add_element(self, element):
        self.sumIutils += element.iutils
        self.sumMutils += element.mutils
        self.elements.append(element)


# ==============================
# ALGORITHM
# ==============================

class HAUIMiner:

    def __init__(self):
        self.mapItemToAUUB = {}
        self.huiCount = 0
        self.writer = None
        self.startTimestamp = 0
        self.endTimestamp = 0

    def run_algorithm(self, input_file, output_file, minAUtility):

        self.startTimestamp = time.time()
        self.writer = open(output_file, "w")

        # ------------------------
        # FIRST DATABASE SCAN
        # ------------------------
        database = []

        with open(input_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                parts = line.split(":")
                items = list(map(int, parts[0].split()))
                utilities = list(map(int, parts[2].split()))

                transaction_max = max(utilities)

                for item in items:
                    self.mapItemToAUUB[item] = \
                        self.mapItemToAUUB.get(item, 0) + transaction_max

                database.append((items, utilities))

        # ------------------------
        # PROMISING ITEMS
        # ------------------------
        promising_items = [
            item for item in self.mapItemToAUUB
            if self.mapItemToAUUB[item] >= minAUtility
        ]

        promising_items.sort(
            key=lambda x: (self.mapItemToAUUB[x], x)
        )

        # ------------------------
        # REVISED DATABASE
        # ------------------------
        revised_db = []

        for items, utilities in database:
            transaction = []
            for i in range(len(items)):
                if self.mapItemToAUUB[items[i]] >= minAUtility:
                    transaction.append((items[i], utilities[i]))

            transaction.sort(
                key=lambda x: (self.mapItemToAUUB[x[0]], x[0])
            )

            revised_db.append(transaction)

        # ------------------------
        # INITIAL UTILITY LISTS
        # ------------------------
        for item in promising_items:
            self.initial_utility_list(
                minAUtility, revised_db, item
            )

        self.writer.close()
        self.endTimestamp = time.time()

    # ==============================

    def initial_utility_list(self, minAUtility, revised_db, item):

        mapItemToUtilityList = {}
        mapItemToAuubList = {}

        # Calculate AUUB again
        for tid, transaction in enumerate(revised_db):
            for i in range(len(transaction)):
                if transaction[i][0] == item:

                    transaction_max = max(
                        [pair[1] for pair in transaction[i:]]
                    )

                    for pair in transaction[i:]:
                        mapItemToAuubList[pair[0]] = \
                            mapItemToAuubList.get(pair[0], 0) + transaction_max
                    break

        # Build utility lists
        for tid, transaction in enumerate(revised_db):
            for i in range(len(transaction)):
                if transaction[i][0] == item:

                    maxUtility = 0
                    for pair in transaction[i:]:
                        if mapItemToAuubList.get(pair[0], 0) >= minAUtility:
                            maxUtility = max(maxUtility, pair[1])

                    for pair in transaction[i:]:

                        if mapItemToAuubList.get(pair[0], 0) >= minAUtility:

                            element = Element(
                                tid,
                                pair[1],
                                maxUtility
                            )

                            if pair[0] not in mapItemToUtilityList:
                                mapItemToUtilityList[pair[0]] = \
                                    UtilityList(pair[0])

                            mapItemToUtilityList[pair[0]].add_element(element)

                    break

        # Keep only promising
        listOfUL = [
            ul for ul in mapItemToUtilityList.values()
            if ul.sumMutils >= minAUtility
        ]

        listOfUL.sort(
            key=lambda ul: (self.mapItemToAUUB[ul.item], ul.item)
        )

        self.hui_miner([], None, listOfUL, minAUtility, 1)

    # ==============================

    def hui_miner(self, prefix, pUL, ULs, minAUtility, length):

        for i in range(len(ULs)):

            X = ULs[i]

            avgUtility = X.sumIutils / length

            if avgUtility >= minAUtility:
                self.write_out(prefix, X.item, avgUtility)

            if X.sumMutils >= minAUtility:

                exULs = []

                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    newUL = self.construct(pUL, X, Y)
                    exULs.append(newUL)

                newPrefix = prefix + [X.item]

                self.hui_miner(
                    newPrefix,
                    X,
                    exULs,
                    minAUtility,
                    length + 1
                )

            if length == 1:
                break

    # ==============================

    def construct(self, P, px, py):

        pxy = UtilityList(py.item)

        for ex in px.elements:

            ey = next(
                (e for e in py.elements if e.tid == ex.tid),
                None
            )

            if ey is None:
                continue

            if P is None:
                newElement = Element(
                    ex.tid,
                    ex.iutils + ey.iutils,
                    ey.mutils
                )
            else:
                e = next(
                    (e for e in P.elements if e.tid == ex.tid),
                    None
                )
                if e is None:
                    continue

                newElement = Element(
                    ex.tid,
                    ex.iutils + ey.iutils - e.iutils,
                    ey.mutils
                )

            pxy.add_element(newElement)

        return pxy

    # ==============================

    def write_out(self, prefix, item, autility):

        self.huiCount += 1

        buffer = ""
        for p in prefix:
            buffer += str(p) + " "
        buffer += str(item)
        buffer += " #AUTIL: "
        buffer += str(autility)

        self.writer.write(buffer + "\n")

    # ==============================

    def print_stats(self):

        print("=============  HAUI-MINER PYTHON =============")
        print("Total time ~", round((self.endTimestamp - self.startTimestamp)*1000,2), "ms")
        print("High-utility itemsets count :", self.huiCount)
        print("===============================================")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":

    miner = HAUIMiner()
    miner.run_algorithm(INPUT_FILE, OUTPUT_FILE, MIN_AUTILITY)
    miner.print_stats()