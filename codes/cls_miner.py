import os
import time
import tracemalloc
import traceback


class AlgoCLS_miner:
    def __init__(self, useChain_EUCP, useCoverage, useLBP, usePreCheck):
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.chuidCount = 0
        self.candidateCount = 0
        self.mapItemToTWU = {}
        self.writer = None
        self.minUtility = 0
        self.count1 = 0
        self.count2 = 0
        self.mapFMAP = None
        self.Cov = None
        self.listItemsetsBySize = None
        self.setOfItemsInClosedItemsets = None
        self.useChain_EUCP = useChain_EUCP
        self.useCoverage = useCoverage
        self.useLBP = useLBP
        self.usePreCheck = usePreCheck

    def saveToMemory(self, itemset, sumIutils, support):
        if len(itemset) >= len(self.listItemsetsBySize):
            i = len(self.listItemsetsBySize)
            while i <= len(itemset):
                self.listItemsetsBySize.append([])
                i += 1
        listToAdd = self.listItemsetsBySize[len(itemset)]
        listToAdd.append(Itemset(itemset, sumIutils, support))
        for item in itemset:
            self.setOfItemsInClosedItemsets.add(item)

    def runAlgorithm(self, input, minUtility, output):
        MemoryLogger.getInstance().reset()
        self.minUtility = minUtility

        if output is not None:
            self.writer = open(output, "w", encoding="utf-8")
        else:
            self.listItemsetsBySize = []
            self.setOfItemsInClosedItemsets = set()

        if self.useChain_EUCP:
            self.mapFMAP = {}
        if self.useCoverage:
            self.Cov = {}

        self.startTimestamp = int(time.time() * 1000)
        self.mapItemToTWU = {}

        try:
            with open(input, "r", encoding="utf-8") as myInput:
                for thisLine in myInput:
                    thisLine = thisLine.strip()
                    if (
                        thisLine == ""
                        or thisLine[0] == "#"
                        or thisLine[0] == "%"
                        or thisLine[0] == "@"
                    ):
                        continue
                    split = thisLine.split(":")
                    items = split[0].split(" ")
                    transactionUtility = int(split[1])
                    for token in items:
                        item = int(token)
                        twu = self.mapItemToTWU.get(item)
                        twu = transactionUtility if twu is None else twu + transactionUtility
                        self.mapItemToTWU[item] = twu
        except Exception:
            traceback.print_exc()

        listOfUtilityLists = []
        mapItemToUtilityList = {}
        for item in self.mapItemToTWU:
            if self.mapItemToTWU[item] >= minUtility:
                uList = UtilityList(item)
                mapItemToUtilityList[item] = uList
                listOfUtilityLists.append(uList)

        listOfUtilityLists.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        try:
            with open(input, "r", encoding="utf-8") as myInput:
                tid = 0
                for thisLine in myInput:
                    thisLine = thisLine.strip()
                    if (
                        thisLine == ""
                        or thisLine[0] == "#"
                        or thisLine[0] == "%"
                        or thisLine[0] == "@"
                    ):
                        continue
                    tid += 1
                    split = thisLine.split(":")
                    items = split[0].split(" ")
                    utilityValues = split[2].split(" ")

                    newTU = 0
                    revisedTransaction = []

                    for i in range(len(items)):
                        pair = PairItemUtility()
                        pair.item = int(items[i])
                        pair.utility = int(utilityValues[i])
                        if self.mapItemToTWU.get(pair.item, 0) >= minUtility:
                            revisedTransaction.append(pair)
                            newTU += pair.utility

                    revisedTransaction.sort(
                        key=lambda p: (self.mapItemToTWU[p.item], p.item)
                    )

                    remainingUtility = newTU
                    for i in range(len(revisedTransaction)):
                        pair = revisedTransaction[i]
                        remainingUtility -= pair.utility

                        utilityListOfItem = mapItemToUtilityList[pair.item]
                        element = Element(tid, pair.utility, remainingUtility)
                        utilityListOfItem.addElement(element)

                        if self.useChain_EUCP:
                            mapFMAPItem = self.mapFMAP.get(pair.item)
                            if mapFMAPItem is None:
                                mapFMAPItem = {}
                                self.mapFMAP[pair.item] = mapFMAPItem

                            for j in range(i + 1, len(revisedTransaction)):
                                pairAfter = revisedTransaction[j]
                                twuSum = mapFMAPItem.get(pairAfter.item)
                                if twuSum is None:
                                    mapFMAPItem[pairAfter.item] = newTU
                                else:
                                    mapFMAPItem[pairAfter.item] = twuSum + newTU
        except Exception:
            traceback.print_exc()

        if self.useCoverage:
            self.CoverageConstructProcedure(listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        self.CLS_Miner(True, [], None, [], listOfUtilityLists)

        MemoryLogger.getInstance().checkMemory()

        if self.writer is not None:
            self.writer.close()

        self.endTimestamp = int(time.time() * 1000)
        return self.listItemsetsBySize

    def CLS_Miner(self, firstTime, closedSet, closedSetUL, preset, postset):
        for iUL in postset:
            if firstTime:
                newgen_TIDs = iUL
            else:
                newgen_TIDs = self.construct(closedSetUL, iUL)

            if self.isPassingHUIPruning(newgen_TIDs):
                newGen = self.appendItem(closedSet, iUL.item)

                if not self.improved_is_dup(newgen_TIDs, preset):
                    closedSetNew = newGen
                    closedsetNewTIDs = newgen_TIDs
                    postsetNew = []
                    passedHUIPruning = True

                    for jUL in postset:
                        if jUL.item == iUL.item or self.compareItems(jUL.item, iUL.item) < 0:
                            continue

                        if self.useLBP and self.calculate_Con(newgen_TIDs, jUL) < self.minUtility:
                            continue

                        if self.useChain_EUCP:
                            shouldpassEUCS = False
                            for item in closedSetNew:
                                shouldpassEUCS = self.checkGenEUCPStrategy(item, jUL.item)
                                if shouldpassEUCS:
                                    break
                            if shouldpassEUCS:
                                self.count1 += 1
                                continue

                        if self.usePreCheck or self.useCoverage:
                            should_merge = (
                                (self.useCoverage and self.ifBelongToCov(iUL.item, jUL.item))
                                or (
                                    self.usePreCheck
                                    and self.preCheckContain(jUL, newgen_TIDs)
                                    and self.containsAllTIDS(jUL, newgen_TIDs)
                                )
                            )
                            if should_merge:
                                closedSetNew = self.appendItem(closedSetNew, jUL.item)
                                closedsetNewTIDs = self.construct(closedsetNewTIDs, jUL)
                                if not self.isPassingHUIPruning(closedsetNewTIDs):
                                    passedHUIPruning = False
                                    break
                            else:
                                postsetNew.append(jUL)
                        else:
                            if self.containsAllTIDS(jUL, newgen_TIDs):
                                closedSetNew = self.appendItem(closedSetNew, jUL.item)
                                closedsetNewTIDs = self.construct(closedsetNewTIDs, jUL)
                                if not self.isPassingHUIPruning(closedsetNewTIDs):
                                    passedHUIPruning = False
                                    break
                            else:
                                postsetNew.append(jUL)

                        self.candidateCount += 1

                    if passedHUIPruning:
                        if closedsetNewTIDs.sumIutils >= self.minUtility:
                            self.saveCHUI(
                                closedSetNew,
                                closedsetNewTIDs.sumIutils,
                                len(closedsetNewTIDs.elements),
                            )

                        presetNew = list(preset)
                        self.CLS_Miner(False, closedSetNew, closedsetNewTIDs, presetNew, postsetNew)

                    preset.append(iUL)

    def isPassingHUIPruning(self, utilitylist):
        return utilitylist.sumIutils + utilitylist.sumRutils >= self.minUtility

    def containsAllTIDS(self, ul1, ul2):
        for elmX in ul2.elements:
            elmE = self.findElementWithTID(ul1, elmX.tid)
            if elmE is None:
                return False
        return True

    def checkEUCPStrategy(self, itemX, itemY):
        mapTWUF = self.mapFMAP.get(itemX)
        if mapTWUF is not None:
            twuF = mapTWUF.get(itemY)
            if twuF is None or twuF < self.minUtility:
                return True
        return False

    def checkGenEUCPStrategy(self, itemX, itemY):
        if self.compareItems(itemX, itemY) > 0:
            itemX, itemY = itemY, itemX
        mapTWUF = self.mapFMAP.get(itemX)
        if mapTWUF is not None:
            twuF = mapTWUF.get(itemY)
            if twuF is None or twuF < self.minUtility:
                return True
        return False

    def appendItem(self, itemset, item):
        newgen = [0] * (len(itemset) + 1)
        newgen[: len(itemset)] = itemset
        newgen[len(itemset)] = item
        return newgen

    def calculate_Con(self, X, Y):
        con = int(X.sumIutils + X.sumRutils - self.cdiff(X, Y) * self.getMinValueofUL(X))
        return con

    def Tidset_diff(self, X, Y):
        return abs(X.getSupport() - Y.getSupport())

    def cdiff(self, X, Y):
        if X.getSupport() < Y.getSupport():
            return 0
        return X.getSupport() - Y.getSupport()

    def getMinValueofUL(self, X):
        minValue = X.elements[0].iutils + X.elements[0].rutils
        for element in X.elements:
            value = element.iutils + element.rutils
            if value < minValue:
                minValue = value
        return minValue

    def improved_is_dup(self, newgenTIDs, preset):
        for j in preset:
            if self.preCheckContain(j, newgenTIDs):
                containsAll = True
                for elmX in newgenTIDs.elements:
                    elmE = self.findElementWithTID(j, elmX.tid)
                    if elmE is None:
                        containsAll = False
                        break
                if containsAll:
                    return True
        return False

    def preCheckContain(self, X, Y):
        lenX = X.getSupport()
        lenY = Y.getSupport()
        if lenX < lenY:
            return False
        for i in range(lenY):
            if X.elements[i].tid > Y.elements[i].tid:
                return False
            if X.elements[lenX - i - 1].tid < Y.elements[lenY - i - 1].tid:
                return False
        return True

    def is_dup(self, newgenTIDs, preset):
        for j in preset:
            containsAll = True
            for elmX in newgenTIDs.elements:
                elmE = self.findElementWithTID(j, elmX.tid)
                if elmE is None:
                    containsAll = False
                    break
            if containsAll:
                return True
        return False

    def construct(self, uX, uE):
        uXE = UtilityList(uE.item)
        for elmX in uX.elements:
            elmE = self.findElementWithTID(uE, elmX.tid)
            if elmE is None:
                continue
            elmXe = Element(elmX.tid, elmX.iutils + elmE.iutils, elmX.rutils - elmE.iutils)
            uXE.addElement(elmXe)
        return uXE

    def findElementWithTID(self, ulist, tid):
        elems = ulist.elements
        first = 0
        last = len(elems) - 1

        while first <= last:
            middle = (first + last) >> 1
            if elems[middle].tid < tid:
                first = middle + 1
            elif elems[middle].tid > tid:
                last = middle - 1
            else:
                return elems[middle]
        return None

    def binarySearch(self, list_values, value):
        first = 0
        last = len(list_values) - 1
        while first <= last:
            middle = (first + last) >> 1
            if int(list_values[middle]) < value:
                first = middle + 1
            elif int(list_values[middle]) > value:
                last = middle - 1
            else:
                return int(list_values[middle])
        return -1

    def CoverageConstructProcedure(self, listOfUtilityLists):
        for ulX in listOfUtilityLists:
            itemX = ulX.item
            mapTWUF = self.mapFMAP.get(itemX) if self.mapFMAP is not None else None
            listofX = []
            for uly in listOfUtilityLists:
                itemY = uly.item
                if itemX == itemY or self.compareItems(itemY, itemX) < 0:
                    continue
                if mapTWUF is not None and itemY in mapTWUF:
                    EUCSXY = mapTWUF[itemY]
                    if EUCSXY == self.mapItemToTWU.get(itemX):
                        listofX.append(itemY)
            self.Cov[itemX] = listofX

    def ifBelongToCov(self, x, y):
        return y in self.Cov.get(x, [])

    def saveCHUI(self, itemset, sumIutils, support):
        self.chuidCount += 1
        if self.writer is None:
            self.saveToMemory(itemset, sumIutils, support)
        else:
            buffer_parts = []
            for item in itemset:
                buffer_parts.append(str(item))
            buffer = " ".join(buffer_parts) + " "
            buffer += "#SUP: " + str(support)
            buffer += " #UTIL: " + str(sumIutils)
            self.writer.write(buffer)
            self.writer.write("\n")

    def printStats(self):
        print("=============  CLS-Miner ALGORITHM SPMF 1.0 - STATS =============")
        print(" Total time ~ " + str(self.endTimestamp - self.startTimestamp) + " ms")
        print(" Memory ~ " + str(MemoryLogger.getInstance().getMaxMemory()) + " MB")
        print(" Closed High-utility itemsets count : " + str(self.chuidCount))
        print(" Candidate count : " + str(self.candidateCount))
        print("=====================================================")

    def compareItems(self, item1, item2):
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return (item1 - item2) if compare == 0 else compare


class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class Itemset:
    def __init__(self, itemset, utility, support):
        self.itemset = itemset
        self.utility = utility
        self.support = support

    def toString(self):
        return str(self.itemset) + " utility : " + str(self.utility) + " support:  " + str(self.support)


class MainTestCLS_Miner:
    @staticmethod
    def fileToPath(filename):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    @staticmethod
    def main(arg=None):
        del arg
        input_path = MainTestCLS_Miner.fileToPath("DB_Utility.txt")
        min_utility = 30
        output = MainTestCLS_Miner.fileToPath("output_python.txt")
        clsMiner = AlgoCLS_miner(True, False, True, True)
        clsMiner.runAlgorithm(input_path, min_utility, output)
        clsMiner.printStats()


class MemoryLogger:
    _instance = None

    def __init__(self):
        self.maxMemory = 0.0
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def getMaxMemory(self):
        return self.maxMemory

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self):
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        currentMemory, _peak = tracemalloc.get_traced_memory()
        currentMemoryMB = currentMemory / 1024.0 / 1024.0
        if currentMemoryMB > self.maxMemory:
            self.maxMemory = currentMemoryMB
        return currentMemoryMB


class PairItemUtility:
    def __init__(self):
        self.item = 0
        self.utility = 0

    def toString(self):
        return "[" + str(self.item) + "," + str(self.utility) + "]"


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

    def getSupport(self):
        return len(self.elements)

    def getUtils(self):
        return self.sumIutils


if __name__ == "__main__":
    MainTestCLS_Miner.main()
