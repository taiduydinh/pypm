import os
import tracemalloc


class AlgoCLHMiner:
    """Combined Python port of the CLH-Miner Java implementation."""

    mapItemToUtilityList = {}

    class Pair:
        def __init__(self, item=0, utility=0.0):
            self.item = item
            self.utility = utility

    def __init__(self):
        self.minUtil = 0
        self.ListUls = []
        self.itemCount = 0
        self.giCount = 0
        self.taxDepth = 0
        self.startTimestamp = 0
        self.mapItemToTWU = {}
        self.endTimestamp = 0
        self.taxonomy = None
        self.itemsetBuffer = []
        self.revisedTransaction = []
        self.datasetAfterRemove = []
        self.countHUI = 0
        self.candidate = 0
        self.writer = None

    def runAlgorithm(self, minUtil, inputPath, outputPath, TaxonomyPath):
        import time

        self.writer = open(outputPath, "w", encoding="utf-8", newline="")
        self.minUtil = minUtil
        self.candidate = 0
        self.startTimestamp = int(time.time() * 1000)
        self.mapItemToTWU = {}
        self.taxonomy = TaxonomyTree()
        self.taxonomy.ReadDataFromPath(TaxonomyPath)
        self.itemsetBuffer = [0] * 500
        self.datasetAfterRemove = []
        self.countHUI = 0
        itemInDB = set()
        MemoryLogger.getInstance().reset()

        with open(inputPath, "r", encoding="utf-8") as myInput:
            for raw_line in myInput:
                thisLine = raw_line.strip()
                if not thisLine or thisLine[0] in "#%@":
                    continue

                split = thisLine.split(":")
                items = split[0].split(" ")
                transactionUtility = float(split[1])
                setParent = set()

                for item_str in items:
                    item = int(item_str)
                    itemInDB.add(item)
                    if self.taxonomy.mapItemToTaxonomyNode.get(item) is None:
                        newNode = TaxonomyNode(item)
                        self.taxonomy.mapItemToTaxonomyNode[-1].addChildren(newNode)
                        self.taxonomy.mapItemToTaxonomyNode[item] = newNode
                    else:
                        parentNode = self.taxonomy.mapItemToTaxonomyNode[item].getParent()
                        while parentNode.getData() != -1:
                            setParent.add(parentNode.getData())
                            parentNode = parentNode.getParent()

                    twu = self.mapItemToTWU.get(item)
                    twu = transactionUtility if twu is None else twu + transactionUtility
                    self.mapItemToTWU[item] = twu

                for parentItemInTransaction in setParent:
                    twu = self.mapItemToTWU.get(parentItemInTransaction)
                    twu = transactionUtility if twu is None else twu + transactionUtility
                    self.mapItemToTWU[parentItemInTransaction] = twu

        listOfUtilityLists = []
        AlgoCLHMiner.mapItemToUtilityList = {}
        for item in self.mapItemToTWU.keys():
            if self.mapItemToTWU[item] >= minUtil:
                uList = UtilityList(item)
                AlgoCLHMiner.mapItemToUtilityList[item] = uList
                listOfUtilityLists.append(uList)

        listOfUtilityLists.sort(key=self._cmp_key_item)

        tid = 0
        with open(inputPath, "r", encoding="utf-8") as myInput:
            for raw_line in myInput:
                thisLine = raw_line.strip()
                if not thisLine or thisLine[0] in "#%@":
                    continue

                split = thisLine.split(":")
                items = split[0].split(" ")
                utilityValues = split[2].split(" ")

                remainingUtility = 0.0
                TU = float(split[1])
                revisedTransaction = []
                mapParentToUtility = {}

                for index, item_str in enumerate(items):
                    utility = float(utilityValues[index])
                    item = int(item_str)
                    nodeParent = self.taxonomy.mapItemToTaxonomyNode[item].getParent()
                    while nodeParent.getData() != -1:
                        utilityOfParent = mapParentToUtility.get(nodeParent.getData())
                        if utilityOfParent is not None:
                            mapParentToUtility[nodeParent.getData()] = utilityOfParent + utility
                        else:
                            mapParentToUtility[nodeParent.getData()] = utility
                        nodeParent = nodeParent.getParent()

                    pair = AlgoCLHMiner.Pair(item, utility)
                    if self.mapItemToTWU[pair.item] >= minUtil:
                        revisedTransaction.append(pair)
                        remainingUtility += pair.utility

                revisedTransaction.sort(key=self._cmp_key_pair)
                countUtility = remainingUtility

                for pair in revisedTransaction:
                    remainingUtility -= pair.utility
                    utilityListOfItem = AlgoCLHMiner.mapItemToUtilityList[pair.item]
                    element = Element(tid, pair.utility, remainingUtility, TU)
                    utilityListOfItem.addElement(element)

                for itemParent in mapParentToUtility.keys():
                    countUtilityOfEachItem = countUtility
                    for currentItem in revisedTransaction:
                        if self.CheckParent(itemParent, currentItem.item):
                            countUtilityOfEachItem -= currentItem.utility
                        else:
                            if self.compareItems(itemParent, currentItem.item) > 0:
                                countUtilityOfEachItem -= currentItem.utility

                    utilityListOfItem = AlgoCLHMiner.mapItemToUtilityList.get(itemParent)
                    if utilityListOfItem is not None:
                        element = Element(
                            tid,
                            mapParentToUtility[itemParent],
                            countUtilityOfEachItem,
                            TU,
                        )
                        utilityListOfItem.addElement(element)

                self.datasetAfterRemove.append(revisedTransaction)
                tid += 1

        listUtilityLevel1 = []
        for ul1 in listOfUtilityLists:
            if self.taxonomy.getMapItemToTaxonomyNode()[ul1.item].getLevel() == 1:
                listUtilityLevel1.append(ul1)
            if self.taxonomy.getMapItemToTaxonomyNode()[ul1.item].getLevel() > 1:
                break

        self.itemCount = len(itemInDB)
        self.giCount = self.taxonomy.getGI() - 1
        self.taxDepth = self.taxonomy.getMaxLevel()

        self.SearchTree(self.itemsetBuffer, 0, None, listUtilityLevel1)
        self.endTimestamp = int(time.time() * 1000)
        self.writer.close()
        self.writer = None
        MemoryLogger.getInstance().checkMemory()

    def SearchTree(self, prefix, prefixLength, pUL, ULs):
        i = 0
        while i < len(ULs):
            X = ULs[i]
            self.candidate += 1

            if X.sumIutils > self.minUtil:
                self.countHUI += 1
                for j in range(prefixLength):
                    self.writer.write(str(prefix[j]) + " ")
                self.writer.write(f"{X.item} #UTIL: {self._format_num(X.sumIutils)}")
                self.writer.write("\n")

            exULs = []
            j = i + 1
            while j < len(ULs):
                Y = ULs[j]
                if not self.CheckParent(Y.item, X.item):
                    exULBuild = self.construct(pUL, X, Y)
                    if exULBuild.GWU > self.minUtil:
                        exULs.append(exULBuild)
                j += 1

            if X.sumIutils + X.sumRutils > self.minUtil:
                taxonomyNodeX = self.taxonomy.getMapItemToTaxonomyNode()[X.item]
                childOfX = taxonomyNodeX.getChildren()
                for taxonomyNode in childOfX:
                    child = taxonomyNode.getData()
                    ULofChild = AlgoCLHMiner.mapItemToUtilityList.get(child)
                    if ULofChild is not None:
                        exULBuild = self.constructTax(pUL, ULofChild)
                        X.AddChild(exULBuild)

                for childULs in X.getChild():
                    if childULs.GWU > self.minUtil:
                        ULs.append(childULs)

            prefix[prefixLength] = X.item
            MemoryLogger.getInstance().checkMemory()
            self.SearchTree(prefix, prefixLength + 1, X, exULs)
            i += 1

    def constructTax(self, P, Child):
        if P is None:
            return Child

        newULs = UtilityList(Child.item)
        for PElement in P.getElement():
            unionChild = self.findElementWithTID(Child, PElement.tid)
            if unionChild is not None:
                trans = self.datasetAfterRemove[unionChild.tid]
                remainUtility = 0.0
                for pair in trans:
                    currentItem = pair.item
                    if (
                        self.compareItems(currentItem, Child.item) > 0
                        and (not self.CheckParent(Child.item, currentItem))
                        and (not self.CheckParent(Child.item, currentItem))
                    ):
                        remainUtility += pair.utility

                newElement = Element(
                    unionChild.tid,
                    PElement.iutils + unionChild.iutils,
                    remainUtility,
                    unionChild.TU,
                )
                newULs.addElement(newElement)
        return newULs

    def construct(self, P, px, py):
        pxyUL = UtilityList(py.item)

        for ex in px.elements:
            ey = self.findElementWithTID(py, ex.tid)
            if ey is None:
                continue

            if P is None:
                trans = self.datasetAfterRemove[ex.tid]
                remainUtility = 0.0
                for pair in trans:
                    currentItem = pair.item
                    if (
                        self.compareItems(currentItem, py.item) > 0
                        and (not self.CheckParent(px.item, currentItem))
                        and (not self.CheckParent(py.item, currentItem))
                    ):
                        remainUtility += pair.utility

                eXY = Element(ex.tid, ex.iutils + ey.iutils, remainUtility, ey.TU)
                pxyUL.addElement(eXY)
            else:
                e = self.findElementWithTID(P, ex.tid)
                if e is not None:
                    trans = self.datasetAfterRemove[e.tid]
                    remainUtility = 0.0
                    for pair in trans:
                        currentItem = pair.item
                        if (
                            self.compareItems(currentItem, py.item) > 0
                            and (not self.CheckParent(px.item, currentItem))
                            and (not self.CheckParent(py.item, currentItem))
                        ):
                            remainUtility += pair.utility

                    eXY = Element(
                        ex.tid,
                        ex.iutils + ey.iutils - e.iutils,
                        remainUtility,
                        ey.TU,
                    )
                    pxyUL.addElement(eXY)

        return pxyUL

    def findElementWithTID(self, ulist, tid):
        elements = ulist.elements
        first = 0
        last = len(elements) - 1
        while first <= last:
            middle = (first + last) >> 1
            middle_tid = elements[middle].tid
            if middle_tid < tid:
                first = middle + 1
            elif middle_tid > tid:
                last = middle - 1
            else:
                return elements[middle]
        return None

    def compareItems(self, item1, item2):
        levelOfItem1 = self.taxonomy.getMapItemToTaxonomyNode()[item1].getLevel()
        levelOfItem2 = self.taxonomy.getMapItemToTaxonomyNode()[item2].getLevel()
        if levelOfItem1 == levelOfItem2:
            compare = int(self.mapItemToTWU[item1] - self.mapItemToTWU[item2])
            return (item1 - item2) if compare == 0 else compare
        return levelOfItem1 - levelOfItem2

    def CheckParent(self, item1, item2):
        nodeItem1 = self.taxonomy.getMapItemToTaxonomyNode()[item1]
        nodeItem2 = self.taxonomy.getMapItemToTaxonomyNode()[item2]
        levelOfItem1 = nodeItem1.getLevel()
        levelOfItem2 = nodeItem2.getLevel()
        if levelOfItem1 == levelOfItem2:
            return False

        if levelOfItem1 > levelOfItem2:
            parentItem1 = nodeItem1.getParent()
            while parentItem1.getData() != -1:
                if parentItem1.getData() == nodeItem2.getData():
                    return True
                parentItem1 = parentItem1.getParent()
            return False

        parentItem2 = nodeItem2.getParent()
        while parentItem2.getData() != -1:
            if parentItem2.getData() == nodeItem1.getData():
                return True
            parentItem2 = parentItem2.getParent()
        return False

    def printStats(self):
        print("=============  CLH-Miner v. 2.45 =============")
        print(f" Runtime time ~ : {self.endTimestamp - self.startTimestamp} ms")
        print(f" Memory ~ : {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" Cross level high utility itemsets (count): {self.countHUI}")
        print(f"   Number of items              : {self.itemCount}")
        print(f"   Number of generalized items             : {self.giCount}")
        print(f"   Taxonomy depth   : {self.taxDepth}")
        print(f"   Candidates (count): {self.candidate}")
        print("======================================")

    def _cmp_key_item(self, utility_list):
        node = self.taxonomy.getMapItemToTaxonomyNode()[utility_list.item]
        level = node.getLevel()
        twu = self.mapItemToTWU[utility_list.item]
        return (level, twu, utility_list.item)

    def _cmp_key_pair(self, pair):
        node = self.taxonomy.getMapItemToTaxonomyNode()[pair.item]
        level = node.getLevel()
        twu = self.mapItemToTWU[pair.item]
        return (level, twu, pair.item)

    def _format_num(self, value):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)


class Element:
    def __init__(self, tid, iutils=0.0, rutils=0.0, TU=0.0):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils
        self.TU = TU


class TaxonomyNode:
    def __init__(self, data):
        self.data = data
        self.children = []
        self.parent = None
        self.level = 0

    def getData(self):
        return self.data

    def setData(self, data):
        self.data = data

    def getChildren(self):
        return self.children

    def setChildren(self, children):
        self.children = children

    def getParent(self):
        return self.parent

    def setParent(self, parent):
        self.parent = parent

    def getLevel(self):
        return self.level

    def setLevel(self, level):
        self.level = level

    def addChildren(self, child):
        if isinstance(child, list):
            for each in child:
                each.setParent(self)
            self.children.extend(child)
            return None
        child.setParent(self)
        self.children.append(child)
        return child


class TaxonomyTree:
    def __init__(self):
        self.Root = TaxonomyNode(-1)
        self.mapItemToTaxonomyNode = {-1: self.Root}
        self.GI = 0
        self.I = 0
        self.MaxLevel = 0

    def getRoot(self):
        return self.Root

    def setRoot(self, root):
        self.Root = root

    def getGI(self):
        return self.GI

    def setGI(self, gI):
        self.GI = gI

    def getI(self):
        return self.I

    def setI(self, i):
        self.I = i

    def getMaxLevel(self):
        return self.MaxLevel

    def setMaxLevel(self, maxLevel):
        self.MaxLevel = maxLevel

    def ReadDataFromPath(self, path):
        reader = open(path, "r", encoding="utf-8")
        try:
            for raw_line in reader:
                line = raw_line.strip()
                if not line or line[0] in "#@":
                    continue
                tokens = line.split(",")
                child = int(tokens[0])
                parent = int(tokens[1])

                nodeParent = self.mapItemToTaxonomyNode.get(parent)
                if nodeParent is None:
                    nodeParent = TaxonomyNode(parent)
                    nodeChildren = self.mapItemToTaxonomyNode.get(child)
                    if nodeChildren is None:
                        nodeChildren = TaxonomyNode(child)
                        nodeParent.addChildren(nodeChildren)
                        self.mapItemToTaxonomyNode[child] = nodeChildren
                    else:
                        nodeParent.addChildren(nodeChildren)
                    self.mapItemToTaxonomyNode[parent] = nodeParent
                else:
                    nodeChildren = self.mapItemToTaxonomyNode.get(child)
                    if nodeChildren is None:
                        nodeChildren = TaxonomyNode(child)
                        nodeParent.addChildren(nodeChildren)
                        self.mapItemToTaxonomyNode[child] = nodeChildren
                    else:
                        nodeParent.addChildren(nodeChildren)
        finally:
            reader.close()
            for item, node in list(self.mapItemToTaxonomyNode.items()):
                if item != -1 and node.getParent() is None:
                    self.Root.addChildren(node)
            self.SetLevelForNode()

    def SetLevelForNode(self):
        self.GI = 0
        self.I = 0
        self.MaxLevel = 0
        for item, node in self.mapItemToTaxonomyNode.items():
            currentLevel = 0
            if item != -1:
                currentLevel = 1
                parent = node.getParent()
                while parent.getData() != -1:
                    currentLevel += 1
                    parent = parent.getParent()
            if len(node.getChildren()) == 0:
                self.I += 1
            else:
                self.GI += 1
            node.setLevel(currentLevel)
            if currentLevel > self.MaxLevel:
                self.MaxLevel = currentLevel

    def getMapItemToTaxonomyNode(self):
        return self.mapItemToTaxonomyNode

    def setMapItemToTaxonomyNode(self, mapItemToTaxonomyNode):
        self.mapItemToTaxonomyNode = mapItemToTaxonomyNode


class UtilityList:
    def __init__(self, item):
        self.item = item
        self.sumIutils = 0.0
        self.sumRutils = 0.0
        self.elements = []
        self.childs = []
        self.GWU = 0.0

    def addElement(self, element):
        self.sumIutils += element.iutils
        self.sumRutils += element.rutils
        self.elements.append(element)
        self.GWU += element.TU

    def getSupport(self):
        return len(self.elements)

    def getElement(self):
        return self.elements

    def getChild(self):
        return self.childs

    def AddChild(self, uLs):
        self.childs.append(uLs)


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
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    def checkMemory(self):
        if tracemalloc.is_tracing():
            current, _peak = tracemalloc.get_traced_memory()
            currentMemory = current / 1024.0 / 1024.0
        else:
            currentMemory = 0.0
        if currentMemory > self.maxMemory:
            self.maxMemory = currentMemory
        return currentMemory


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    taxonomyPath = os.path.join(base, "taxonomy_CLHMiner.txt")
    inputPath = os.path.join(base, "transaction_CLHMiner.txt")
    outputPath = os.path.join(base, "output_python.txt")
    minimumUtility = 40

    algo = AlgoCLHMiner()
    algo.runAlgorithm(minimumUtility, inputPath, outputPath, taxonomyPath)
    algo.printStats()


if __name__ == "__main__":
    main()
