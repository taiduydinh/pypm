import os
import time
import tracemalloc
from functools import cmp_to_key


class AlgoSFUI_UF:
    class Pair:
        def __init__(self):
            self.item = 0
            self.utility = 0

    def __init__(self):
        self.maxMemory = 0.0
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.psfupCount = 0
        self.sfupCount = 0
        self.searchCount = 0
        self.MUS = 0
        self.fMax = 0
        self.mapItemToTWU = {}
        self.mapItemToUtility = {}
        self.mapItemToTempTWU = {}
        self.mapItemToFrequent = {}
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    def runAlgorithm(self, input_path, output_path):
        self.maxMemory = 0
        self.startTimestamp = int(time.time() * 1000)
        self.mapItemToTWU = {}
        self.mapItemToUtility = {}
        self.mapItemToTempTWU = {}
        self.mapItemToFrequent = {}
        self.psfupCount = 0
        self.sfupCount = 0
        self.searchCount = 0
        self.MUS = 0
        self.fMax = 0

        unpromising_list = []

        # First pass: compute utility/frequency and temporary TWU for each item
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                thisLine = line.strip()
                if thisLine == "" or thisLine[0] in "#%@":
                    continue
                split = thisLine.split(":")
                items = split[0].split(" ")
                utilities = split[2].split(" ")
                transactionUtility = int(split[1])

                for i in range(len(items)):
                    item = int(items[i])
                    utility = int(utilities[i])

                    u = self.mapItemToUtility.get(item)
                    f_item = self.mapItemToFrequent.get(item)
                    twu = self.mapItemToTempTWU.get(item)

                    self.mapItemToUtility[item] = utility if u is None else u + utility
                    self.mapItemToFrequent[item] = 1 if f_item is None else f_item + 1
                    self.mapItemToTempTWU[item] = transactionUtility if twu is None else twu + transactionUtility

        for item in self.mapItemToFrequent.keys():
            f_item = self.mapItemToFrequent[item]
            u = self.mapItemToUtility[item]
            if f_item > self.fMax:
                self.fMax = f_item
                self.MUS = u
            elif f_item == self.fMax and u > self.MUS:
                self.MUS = u

        for item in self.mapItemToTempTWU.keys():
            if self.mapItemToTempTWU[item] < self.MUS:
                unpromising_list.append(item)

        # Second pass: compute TWU of promising items
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                thisLine = line.strip()
                if thisLine == "" or thisLine[0] in "#%@":
                    continue
                split = thisLine.split(":")
                items = split[0].split(" ")
                transactionUtility = int(split[1])

                for s in items:
                    item = int(s)
                    if item in unpromising_list:
                        continue
                    twu = self.mapItemToTWU.get(item)
                    self.mapItemToTWU[item] = transactionUtility if twu is None else twu + transactionUtility

        # Initialize utility lists
        UtilityLists = []
        mapItemToUtilityList = {}
        for item in self.mapItemToTWU.keys():
            uList = UtilityList(item)
            mapItemToUtilityList[item] = uList
            UtilityLists.append(uList)

        UtilityLists.sort(key=cmp_to_key(lambda o1, o2: self.compareItems(o1.item, o2.item)))

        # Third pass: build utility lists
        tid = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                thisLine = line.strip()
                if thisLine == "" or thisLine[0] in "#%@":
                    continue

                split = thisLine.split(":")
                items = split[0].split(" ")
                utilityValues = split[2].split(" ")
                remainingUtility = 0
                revisedTransaction = []

                for i in range(len(items)):
                    pair = AlgoSFUI_UF.Pair()
                    pair.item = int(items[i])
                    pair.utility = int(utilityValues[i])
                    if pair.item not in unpromising_list:
                        revisedTransaction.append(pair)
                        remainingUtility += pair.utility

                revisedTransaction.sort(key=cmp_to_key(lambda o1, o2: self.compareItems(o1.item, o2.item)))

                for pair in revisedTransaction:
                    remainingUtility -= pair.utility
                    utilityListOfItem = mapItemToUtilityList[pair.item]
                    element = Element(tid, pair.utility, remainingUtility)
                    utilityListOfItem.addElement(element)

                tid += 1

        self.checkMemory()

        MUA = [0] * (self.fMax + 1)
        for i in range(1, self.fMax + 1):
            MUA[i] = self.MUS - 1

        psfupList = [None] * (tid + 1)
        skylineList = []

        self.SFUPMiner([], None, UtilityLists, psfupList, MUA, 0)
        self.SFUSMiner(skylineList, psfupList)
        self.writeOut(skylineList, output_path)
        self.psfupCount = self.getpsfupCount(psfupList)
        self.checkMemory()
        self.endTimestamp = int(time.time() * 1000)

    def compareItems(self, item1, item2):
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else compare

    def SFUPMiner(self, prefix, pUL, ULs, psfupList, MUA, MUE):
        for i in range(len(ULs)):
            X = ULs[i]
            self.searchCount += 1

            if i == 0:
                isPass = X.sumIutils >= MUE
            else:
                isPass = (X.sumIutils + X.sumRutils) >= self.MUS

            if not isPass:
                continue

            MUE = X.sumIutils
            temp = len(X.elements)

            for k in range(temp, len(MUA)):
                if MUA[k] > MUA[temp]:
                    MUA[temp] = MUA[k]

            for t in range(temp - 1, 0, -1):
                if MUA[t] < MUA[temp]:
                    MUA[t] = MUA[temp]
                else:
                    break

            if X.sumIutils == MUA[temp] and MUA[temp] != 0:
                tempPoint = Skyline()
                tempPoint.itemSet = self.itemSetString(prefix, X.item)
                tempPoint.frequent = temp
                tempPoint.utility = X.sumIutils
                if psfupList[temp] is None:
                    psfupList[temp] = SkylineList()
                psfupList[temp].add(tempPoint)

            if X.sumIutils > MUA[temp]:
                MUA[temp] = X.sumIutils
                if psfupList[temp] is None:
                    tempList = SkylineList()
                    tempPoint = Skyline()
                    tempPoint.itemSet = self.itemSetString(prefix, X.item)
                    tempPoint.frequent = temp
                    tempPoint.utility = X.sumIutils
                    tempList.add(tempPoint)
                    psfupList[temp] = tempList
                else:
                    templength = psfupList[temp].size()
                    if templength == 1:
                        psfupList[temp].get(0).itemSet = self.itemSetString(prefix, X.item)
                        psfupList[temp].get(0).utility = X.sumIutils
                    else:
                        for j in range(templength - 1, 0, -1):
                            psfupList[temp].remove(j)
                        psfupList[temp].get(0).itemSet = self.itemSetString(prefix, X.item)
                        psfupList[temp].get(0).utility = X.sumIutils

            if X.sumIutils + X.sumRutils >= MUA[temp] and MUA[temp] != 0:
                exULs = []
                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    exULs.append(self.construct(pUL, X, Y))

                newPrefix = [0] * (len(prefix) + 1)
                for pidx in range(len(prefix)):
                    newPrefix[pidx] = prefix[pidx]
                newPrefix[len(prefix)] = X.item

                self.SFUPMiner(newPrefix, X, exULs, psfupList, MUA, MUE)

    def construct(self, P, px, py):
        pxyUL = UtilityList(py.item)
        for ex in px.elements:
            ey = self.findElementWithTID(py, ex.tid)
            if ey is None:
                continue

            if P is None:
                eXY = Element(ex.tid, ex.iutils + ey.iutils, ey.rutils)
                pxyUL.addElement(eXY)
            else:
                e = self.findElementWithTID(P, ex.tid)
                if e is not None:
                    eXY = Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils)
                    pxyUL.addElement(eXY)
        return pxyUL

    def findElementWithTID(self, ulist, tid):
        lst = ulist.elements
        first = 0
        last = len(lst) - 1
        while first <= last:
            middle = (first + last) >> 1
            if lst[middle].tid < tid:
                first = middle + 1
            elif lst[middle].tid > tid:
                last = middle - 1
            else:
                return lst[middle]
        return None

    def itemSetString(self, prefix, item):
        parts = []
        for value in prefix:
            parts.append(str(value))
        parts.append(str(item))
        return " ".join(parts)

    def writeOut(self, skylineList, output_path):
        self.sfupCount = len(skylineList)
        with open(output_path, "w", encoding="utf-8") as writer:
            for i in range(self.sfupCount):
                sk = skylineList[i]
                writer.write(f"{sk.itemSet} #SUP:{sk.frequent} #UTILITY:{sk.utility}\n")

    def SFUSMiner(self, skylineList, psfupList):
        for i in range(1, len(psfupList)):
            temp = 0
            if psfupList[i] is not None:
                j = i + 1
                while j < len(psfupList):
                    if psfupList[j] is None:
                        j += 1
                    else:
                        if psfupList[i].get(0).utility <= psfupList[j].get(0).utility:
                            temp = 1
                            break
                        j += 1
                if temp == 0:
                    for k in range(psfupList[i].size()):
                        skylineList.append(psfupList[i].get(k))

    def getpsfupCount(self, psfupList):
        for i in range(1, len(psfupList)):
            if psfupList[i] is not None:
                self.psfupCount = self.psfupCount + psfupList[i].size()
        return self.psfupCount

    def checkMemory(self):
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, _peak = tracemalloc.get_traced_memory()
        currentMemory = current / 1024.0 / 1024.0
        if currentMemory > self.maxMemory:
            self.maxMemory = currentMemory

    def printStats(self):
        print("=============  SFUI-UF ALGORITHM v2.50  =============")
        print(" Total time ~ " + str(self.endTimestamp - self.startTimestamp) + " ms")
        print(" Memory ~ " + str(self.maxMemory) + " MB")
        print(" Skyline itemsets count : " + str(self.sfupCount))
        print(" Search itemsets count : " + str(self.searchCount))
        print(" Candidate itemsets count : " + str(self.psfupCount))
        print("===================================================")


class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class MainTestSFUI_UF:
    @staticmethod
    def fileToPath(filename):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    @staticmethod
    def main(_arg=None):
        input_path = MainTestSFUI_UF.fileToPath("DB_Utility.txt")
        output_path = MainTestSFUI_UF.fileToPath("output_python.txt")
        algorithm = AlgoSFUI_UF()
        algorithm.runAlgorithm(input_path, output_path)
        algorithm.printStats()


class Skyline:
    def __init__(self):
        self.itemSet = ""
        self.frequent = 0
        self.utility = 0


class SkylineList:
    def __init__(self):
        self.skylinelist = []

    def get(self, index):
        return self.skylinelist[index]

    def add(self, e):
        self.skylinelist.append(e)

    def remove(self, index):
        del self.skylinelist[index]

    def size(self):
        return len(self.skylinelist)


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


if __name__ == "__main__":
    MainTestSFUI_UF.main()
