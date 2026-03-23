import math


# ------------------------ FP-Tree structures ------------------------ #

class FPNode:
    def __init__(self):
        self.itemID = -1
        self.counter = 1
        self.parent = None
        self.childs = []
        self.nodeLink = None

    def getChildWithID(self, id_):
        for child in self.childs:
            if child.itemID == id_:
                return child
        return None


class FPTree:
    def __init__(self):
        self.headerList = []
        self.mapItemNodes = {}
        self.mapItemLastNode = {}
        self.root = FPNode()

    def fixNodeLinks(self, item, newNode):
        lastNode = self.mapItemLastNode.get(item)
        if lastNode is not None:
            lastNode.nodeLink = newNode
        self.mapItemLastNode[item] = newNode
        if item not in self.mapItemNodes:
            self.mapItemNodes[item] = newNode

    def addTransaction(self, transaction):
        current = self.root
        for item in transaction:
            child = current.getChildWithID(item)
            if child is None:
                new = FPNode()
                new.itemID = item
                new.parent = current
                current.childs.append(new)
                current = new
                self.fixNodeLinks(item, new)
            else:
                child.counter += 1
                current = child

    def addPrefixPath(self, prefixPath, mapSupportBeta, relativeMinsupp):
        # first element of prefixPath stores path support
        pathCount = prefixPath[0].counter
        current = self.root
        # iterate backward (except first element)
        for i in range(len(prefixPath) - 1, 0, -1):
            pathItem = prefixPath[i]
            if mapSupportBeta.get(pathItem.itemID, 0) >= relativeMinsupp:
                child = current.getChildWithID(pathItem.itemID)
                if child is None:
                    new = FPNode()
                    new.itemID = pathItem.itemID
                    new.parent = current
                    new.counter = pathCount
                    current.childs.append(new)
                    current = new
                    self.fixNodeLinks(pathItem.itemID, new)
                else:
                    child.counter += pathCount
                    current = child

    def createHeaderList(self, mapSupport):
        # sort by decreasing support, then by item id ascending
        self.headerList = list(self.mapItemNodes.keys())
        self.headerList.sort(key=lambda id_: (-mapSupport[id_], id_))


# ------------------------ CFI-Tree structures ------------------------ #

class CFINode:
    def __init__(self):
        self.itemID = -1
        self.counter = 1
        self.level = 0
        self.parent = None
        self.childs = []
        self.nodeLink = None

    def getChildWithID(self, id_):
        for child in self.childs:
            if child.itemID == id_:
                return child
        return None


class CFITree:
    def __init__(self):
        self.mapItemNodes = {}
        self.mapItemLastNode = {}
        self.root = CFINode()
        self.lastAddedItemsetNode = None
        self.comparatorOriginalOrder = None  # not used directly in checks

    def setComparator(self, comp):
        self.comparatorOriginalOrder = comp

    def fixNodeLinks(self, item, newNode):
        lastNode = self.mapItemLastNode.get(item)
        if lastNode is not None:
            lastNode.nodeLink = newNode
        self.mapItemLastNode[item] = newNode
        if item not in self.mapItemNodes:
            self.mapItemNodes[item] = newNode

    def addCFI(self, itemset, length, support):
        current = self.root
        for i in range(length):
            item = itemset[i]
            child = current.getChildWithID(item)
            if child is None:
                new = CFINode()
                new.itemID = item
                new.parent = current
                new.level = i + 1
                new.counter = support
                current.childs.append(new)
                current = new
                self.fixNodeLinks(item, new)
            else:
                # keep max support on this path
                child.counter = max(child.counter, support)
                current = child
        self.lastAddedItemsetNode = current

    def issASubsetOfPrefixPath(self, head, headLength, node):
        # test if "head" (length headLength) is contained in the path ending at node
        if node.level >= headLength:
            nodeToCheck = node
            pos = len(head) - 1
            itemToLookFor = head[pos]
            while nodeToCheck is not None:
                if nodeToCheck.itemID == itemToLookFor:
                    pos -= 1
                    if pos < 0:
                        return True
                    itemToLookFor = head[pos]
                nodeToCheck = nodeToCheck.parent
        return False

    def passSubsetChecking(self, head, head_length, support):
        # optimization: check last added itemset first
        if (
            self.lastAddedItemsetNode is not None
            and self.lastAddedItemsetNode.counter == support
        ):
            if self.issASubsetOfPrefixPath(head, head_length, self.lastAddedItemsetNode):
                return False

        # find linked list of nodes for last item of head
        lastItem = head[-1]
        node = self.mapItemNodes.get(lastItem)
        if node is None:
            return True

        while node is not None:
            if node.counter == support and self.issASubsetOfPrefixPath(
                head, head_length, node
            ):
                return False
            node = node.nodeLink

        return True


# ------------------------ Itemset structures (for in-memory use) ------------------------ #

class Itemset:
    def __init__(self, items):
        self.items = list(items)
        self.support = 0

    def setAbsoluteSupport(self, sup):
        self.support = sup


class Itemsets:
    def __init__(self, name):
        self.name = name
        self.levels = [[]]  # index 0 unused, levels[k] = itemsets of size k
        self.itemsetsCount = 0

    def addItemset(self, itemset, k):
        while len(self.levels) <= k:
            self.levels.append([])
        self.levels[k].append(itemset)
        self.itemsetsCount += 1

    def printItemsets(self):
        for k, level in enumerate(self.levels):
            if k == 0:
                continue
            for it in level:
                print(" ".join(map(str, it.items)), "#SUP:", it.support)


# ------------------------ FPClose algorithm ------------------------ #

class AlgoFPClose:
    BUFFERS_SIZE = 2000
    DEBUG = False

    def __init__(self):
        self.transactionCount = 0
        self.itemsetCount = 0
        self.minSupportRelative = 0
        self.writer = None
        self.patterns = None
        self.itemsetBuffer = None
        self.countBuffer = None
        self.cfiTree = None
        self.originalMapSupport = None

    # comparator: support descending, then item ascending
    def comparatorOriginalOrder(self, item1, item2):
        compare = self.originalMapSupport[item2] - self.originalMapSupport[item1]
        if compare == 0:
            compare = item1 - item2
        return compare

    def sortOriginalOrder(self, arr, length):
        # bubble sort like Java version
        for i in range(length):
            for j in range(length - 1, i, -1):
                if self.comparatorOriginalOrder(arr[j], arr[j - 1]) < 0:
                    arr[j], arr[j - 1] = arr[j - 1], arr[j]

    def scanDatabaseToDetermineFrequencyOfSingleItems(self, path):
        sup = {}
        self.transactionCount = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                for tok in line.split():
                    item = int(tok)
                    sup[item] = sup.get(item, 0) + 1
                self.transactionCount += 1
        return sup

    def runAlgorithm(self, input_path, output_path, minsupp):
        # init stats
        self.itemsetCount = 0

        # output choice
        if output_path is None:
            self.writer = None
            self.patterns = Itemsets("FREQUENT ITEMSETS")
        else:
            self.patterns = None
            self.writer = open(output_path, "w", encoding="utf-8")

        # first DB scan
        self.originalMapSupport = self.scanDatabaseToDetermineFrequencyOfSingleItems(
            input_path
        )
        # relative minsup
        self.minSupportRelative = math.ceil(minsupp * self.transactionCount)

        # build initial FP-tree
        self.cfiTree = CFITree()
        tree = FPTree()

        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                trans = []
                for tok in line.split():
                    item = int(tok)
                    if self.originalMapSupport[item] >= self.minSupportRelative:
                        trans.append(item)
                # sort by decreasing support then item id
                trans.sort(key=lambda x: (-self.originalMapSupport[x], x))
                tree.addTransaction(trans)

        self.cfiTree.setComparator(self.comparatorOriginalOrder)
        tree.createHeaderList(self.originalMapSupport)

        if len(tree.headerList) > 0:
            self.itemsetBuffer = [0] * self.BUFFERS_SIZE
            self.countBuffer = [0] * self.BUFFERS_SIZE
            self.fpclose(
                tree, self.itemsetBuffer, 0, self.transactionCount, self.originalMapSupport
            )

        if self.writer is not None:
            self.writer.close()

        return self.patterns

    def saveItemset(self, itemset, length, support):
        itemsetCopy = list(itemset[:length])
        # sort according to original total order
        self.sortOriginalOrder(itemsetCopy, length)

        # add to CFI-tree
        self.cfiTree.addCFI(itemsetCopy, len(itemsetCopy), support)
        self.itemsetCount += 1

        if self.writer is not None:
            # same format as Java: items then " #SUP: x"
            line = " ".join(str(x) for x in itemsetCopy[:length]) + " #SUP: " + str(
                support
            )
            self.writer.write(line + "\n")
        else:
            # if kept in memory, sort lexicographically for display
            lex_sorted = sorted(itemsetCopy[:length])
            it = Itemset(lex_sorted)
            it.setAbsoluteSupport(support)
            self.patterns.addItemset(it, length)

    def fpclose(self, tree, prefix, prefixLength, prefixSupport, mapSupport):
        # 1) check if the FP-tree is a single path
        singlePath = True
        position = prefixLength

        if len(tree.root.childs) > 1:
            singlePath = False
        else:
            if tree.root.childs:
                currentNode = tree.root.childs[0]
                while True:
                    if len(currentNode.childs) > 1:
                        singlePath = False
                        break
                    self.itemsetBuffer[position] = currentNode.itemID
                    self.countBuffer[position] = currentNode.counter
                    position += 1
                    if len(currentNode.childs) == 0:
                        break
                    currentNode = currentNode.childs[0]

        # ---- Case 1: single path ----
        if singlePath and self.countBuffer[position - 1] >= self.minSupportRelative:
            for i in range(prefixLength, position + 1):
                if i == position:
                    pathSupport = self.countBuffer[i - 1]
                    headWithP = self.itemsetBuffer[:i]
                    self.sortOriginalOrder(headWithP, i)
                    if self.cfiTree.passSubsetChecking(headWithP, i, pathSupport):
                        self.saveItemset(headWithP, i, pathSupport)
                else:
                    if (
                        i > 0
                        and self.countBuffer[i - 1] != 0
                        and self.countBuffer[i - 1] != self.countBuffer[i]
                    ):
                        pathSupport = self.countBuffer[i - 1]
                        headWithP = self.itemsetBuffer[:i]
                        self.sortOriginalOrder(headWithP, i)
                        if self.cfiTree.passSubsetChecking(
                            headWithP, i, pathSupport
                        ):
                            self.saveItemset(headWithP, i, pathSupport)

        # ---- Case 2: multiple paths ----
        else:
            # items in header list, reverse order
            for idx in range(len(tree.headerList) - 1, -1, -1):
                item = tree.headerList[idx]
                support = mapSupport[item]
                betaSupport = prefixSupport if prefixSupport < support else support

                prefix[prefixLength] = item
                self.countBuffer[prefixLength] = betaSupport

                # (A) construct conditional pattern base
                prefixPaths = []
                path = tree.mapItemNodes.get(item)
                mapSupportBeta = {}

                while path is not None:
                    if path.parent.itemID != -1:
                        prefixPath = [path]
                        pathCount = path.counter
                        parent = path.parent
                        while parent.itemID != -1:
                            prefixPath.append(parent)
                            mapSupportBeta[parent.itemID] = (
                                mapSupportBeta.get(parent.itemID, 0) + pathCount
                            )
                            parent = parent.parent
                        prefixPaths.append(prefixPath)
                    path = path.nodeLink

                # build head U {item} and sort
                headWithP = prefix[: prefixLength + 1]
                self.sortOriginalOrder(headWithP, prefixLength + 1)

                if self.cfiTree.passSubsetChecking(
                    headWithP, prefixLength + 1, betaSupport
                ):
                    # (B) build conditional FP-tree
                    treeBeta = FPTree()
                    for prefixPath in prefixPaths:
                        treeBeta.addPrefixPath(
                            prefixPath, mapSupportBeta, self.minSupportRelative
                        )

                    if len(treeBeta.root.childs) > 0:
                        treeBeta.createHeaderList(self.originalMapSupport)
                        self.fpclose(
                            treeBeta,
                            prefix,
                            prefixLength + 1,
                            betaSupport,
                            mapSupportBeta,
                        )

                    # try to save headWithP as CFI
                    if self.cfiTree.passSubsetChecking(
                        headWithP, prefixLength + 1, betaSupport
                    ):
                        self.saveItemset(headWithP, prefixLength + 1, betaSupport)

    def getDatabaseSize(self):
        return self.transactionCount

    def printStats(self):
        print("=============  FP-Close (Python) - STATS =============")
        print(" Transactions count from database : ", self.transactionCount)
        print(" Closed frequent itemset count     : ", self.itemsetCount)
        print("======================================================")


# ------------------------ Convenience runner ------------------------ #

def run_fpclose_and_save(
    input_path="contextPasquier99.txt",
    output_path="fpclose_outputs.txt",
    minsup=0.4,
):
    algo = AlgoFPClose()
    algo.runAlgorithm(input_path, output_path, minsup)
    algo.printStats()
    print(f"Closed frequent itemsets written to: {output_path}")


if __name__ == "__main__":
    # Change paths if needed
    input_path = "src/apriori/contextPasquier99.txt"       # <-- string (no comma)
    output_path = "src/apriori/results_apriori.txt"        # <-- string (no comma)

    minsup = 0.4

    algo = AlgoApriori()
    result = algo.runAlgorithm(minsup, input_path, output_path)
    algo.printStats()

    if result is not None:
        result.printItemsets(algo.getDatabaseSize())
