import os
import time

# ================= SETTINGS =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "src", "contextHUIM.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "#100_output.txt")
# ===========================================


# ---------- DATA STRUCTURES ----------

class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class UtilityList:
    def __init__(self, item):
        self.item = item
        self.sumIutils = 0
        self.sumRutils = 0
        self.elements = []

    def addElement(self, element):
        self.sumIutils += element.iutils
        self.sumRutils += element.rutils
        self.elements.append(element)


class Skyline:
    def __init__(self, itemSet, frequent, utility):
        self.itemSet = itemSet
        self.frequent = frequent
        self.utility = utility


class SkylineList:
    def __init__(self):
        self.skylinelist = []

    def add(self, e):
        self.skylinelist.append(e)

    def get(self, i):
        return self.skylinelist[i]

    def remove(self, i):
        del self.skylinelist[i]

    def size(self):
        return len(self.skylinelist)


# ---------- MAIN ALGORITHM ----------

class SFUPMinerUemax:

    def __init__(self):
        self.mapItemToTWU = {}
        self.searchCount = 0
        self.sfupCount = 0
        self.psfupCount = 0

    def compareItems(self, item1, item2):
        diff = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return diff if diff != 0 else item1 - item2

    def runAlgorithm(self):

        start = time.time()

        # -------- FIRST PASS (TWU) --------
        with open(INPUT_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = list(map(int, split[0].split()))
                transactionUtility = int(split[1])

                for item in items:
                    self.mapItemToTWU[item] = self.mapItemToTWU.get(item, 0) + transactionUtility

        # -------- CREATE UTILITY LISTS --------
        mapItemToUtilityList = {}
        listOfUtilityLists = []

        for item in self.mapItemToTWU:
            ul = UtilityList(item)
            mapItemToUtilityList[item] = ul
            listOfUtilityLists.append(ul)

        listOfUtilityLists.sort(key=lambda x: (self.mapItemToTWU[x.item], x.item))

        # -------- SECOND PASS --------
        tid = 0
        with open(INPUT_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                split = line.split(":")
                items = list(map(int, split[0].split()))
                utils = list(map(int, split[2].split()))

                pairs = list(zip(items, utils))
                pairs.sort(key=lambda x: (self.mapItemToTWU[x[0]], x[0]))

                remainingUtility = sum(utils)

                for item, util in pairs:
                    remainingUtility -= util
                    ul = mapItemToUtilityList[item]
                    ul.addElement(Element(tid, util, remainingUtility))

                tid += 1

        uEmax = [0] * (tid + 1)
        psfupList = [None] * (tid + 1)
        skylineList = []

        self.SFUPMiner([], None, listOfUtilityLists, psfupList, skylineList, uEmax)
        self.judgeSkyline(skylineList, psfupList)

        self.writeOutput(skylineList)

        end = time.time()

        print("Finished in", round((end - start) * 1000, 2), "ms")
        print("Skyline count:", len(skylineList))
        print("Search count:", self.searchCount)

    # ---------- RECURSION ----------

    def SFUPMiner(self, prefix, pUL, ULs, psfupList, skylineList, uEmax):

        for i in range(len(ULs)):
            X = ULs[i]
            self.searchCount += 1
            freq = len(X.elements)

            if X.sumIutils > uEmax[freq]:
                uEmax[freq] = X.sumIutils
                psfupList[freq] = SkylineList()
                psfupList[freq].add(Skyline(prefix + [X.item], freq, X.sumIutils))

            elif X.sumIutils == uEmax[freq] and uEmax[freq] != 0:
                psfupList[freq].add(Skyline(prefix + [X.item], freq, X.sumIutils))

            if X.sumIutils + X.sumRutils >= uEmax[freq] and uEmax[freq] != 0:
                exULs = []
                for j in range(i + 1, len(ULs)):
                    exULs.append(self.construct(pUL, X, ULs[j]))

                self.SFUPMiner(prefix + [X.item], X, exULs, psfupList, skylineList, uEmax)

    def construct(self, P, px, py):

        pxyUL = UtilityList(py.item)

        for ex in px.elements:
            ey = next((e for e in py.elements if e.tid == ex.tid), None)
            if ey is None:
                continue

            if P is None:
                pxyUL.addElement(Element(ex.tid, ex.iutils + ey.iutils, ey.rutils))
            else:
                e = next((e for e in P.elements if e.tid == ex.tid), None)
                if e:
                    pxyUL.addElement(Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils))

        return pxyUL

    def judgeSkyline(self, skylineList, psfupList):

        for i in range(1, len(psfupList)):
            if psfupList[i] is None:
                continue

            dominated = False
            for j in range(i + 1, len(psfupList)):
                if psfupList[j] is None:
                    continue
                if psfupList[i].get(0).utility <= psfupList[j].get(0).utility:
                    dominated = True
                    break

            if not dominated:
                for k in range(psfupList[i].size()):
                    skylineList.append(psfupList[i].get(k))

    def writeOutput(self, skylineList):

        with open(OUTPUT_FILE, "w") as f:
            for s in skylineList:
                f.write(" ".join(map(str, s.itemSet)))
                f.write(" #SUP:")
                f.write(str(s.frequent))
                f.write(" #UTILITY:")
                f.write(str(s.utility))
                f.write("\n")


# ---------- RUN ----------

if __name__ == "__main__":
    miner = SFUPMinerUemax()
    miner.runAlgorithm()