import os
import tracemalloc


class AbstractItemsetTree:
    def __init__(self):
        self.root = None
        self.nodeCount = 0
        self.totalItemCountInNodes = 0
        self.startTimestamp = 0
        self.endTimestamp = 0

    def getLargestCommonAncestor(self, itemset1, itemset2):
        if itemset2 is None or itemset1 is None:
            return None
        min_i = min(len(itemset1), len(itemset2))
        count = 0
        for i in range(min_i):
            if itemset1[i] != itemset2[i]:
                break
            count += 1
        if count > 0 and count < min_i:
            return itemset1[:count]
        return None

    def ancestorOf(self, itemset1, itemset2):
        if itemset2 is None:
            return False
        if itemset1 is None:
            return True
        if len(itemset1) >= len(itemset2):
            return False
        for i in range(len(itemset1)):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    def same(self, itemset1, itemset2):
        if itemset2 is None or itemset1 is None:
            return False
        if len(itemset1) != len(itemset2):
            return False
        for i in range(len(itemset1)):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    def getFrequentItemsetSubsumingWithMinSup(self, iset, minsup):
        hashTable = self.getFrequentItemsetSubsuming(iset)
        for lst in hashTable.table:
            if lst is not None:
                i = 0
                while i < len(lst):
                    if lst[i].support < minsup:
                        lst.pop(i)
                    else:
                        i += 1
        return hashTable

    def generateRules(self, s, minsup, minconf):
        rules = []
        seti = set(s)
        suppS = self.getSupportOfItemset(s)
        frequentItemsets = self.getFrequentItemsetSubsumingWithMinSup(s, minsup)
        for lst in frequentItemsets.table:
            if lst is not None:
                for c in lst:
                    if c.size() == len(s):
                        continue
                    consequent = [item for item in c.itemset if item not in seti]
                    suppC = self.getSupportOfItemset(c.itemset)
                    conf = float(suppC) / float(suppS)
                    if conf >= minconf:
                        rule = AssociationRuleIT()
                        rule.itemset1 = list(s)
                        rule.itemset2 = consequent
                        rule.support = suppC
                        rule.confidence = conf
                        rules.append(rule)
        return rules

    def getFrequentItemsetSubsuming(self, s):
        raise NotImplementedError

    def getSupportOfItemset(self, s):
        raise NotImplementedError


class AssociationRuleIT:
    def __init__(self):
        self.support = 0
        self.confidence = 0.0
        self.itemset1 = []
        self.itemset2 = []

    def __str__(self):
        buffer = []
        buffer.append("[ ")
        for item in self.itemset1:
            buffer.append(str(item))
            buffer.append(" ")
        buffer.append(" ] ==> [")
        for item in self.itemset2:
            buffer.append(str(item))
            buffer.append(" ")
        buffer.append(" ]  #SUP: ")
        buffer.append(str(self.support))
        buffer.append("  #CONF:")
        buffer.append(str(self.confidence))
        return "".join(buffer)


class HashTableIT:
    def __init__(self, size):
        self.table = [None] * size

    def put(self, items, support):
        hashcode = self.hashCode(items)
        if self.table[hashcode] is None:
            self.table[hashcode] = []
            itemset = Itemset()
            itemset.itemset = list(items)
            itemset.support = support
            self.table[hashcode].append(itemset)
        else:
            for existingItemset in self.table[hashcode]:
                if self.same(items, existingItemset.itemset):
                    existingItemset.support += support
                    return
            itemset = Itemset()
            itemset.itemset = list(items)
            itemset.support = support
            self.table[hashcode].append(itemset)

    def hashCode(self, items):
        hashcode = 0
        for i in range(len(items)):
            hashcode += items[i] + (i * 10)
        if hashcode < 0:
            hashcode = -hashcode
        # Keep the Java implementation bug/behavior.
        return hashcode % len(items)

    def same(self, itemset1, itemset2):
        if itemset2 is None or itemset1 is None:
            return False
        if len(itemset1) != len(itemset2):
            return False
        for i in range(len(itemset1)):
            if itemset1[i] != itemset2[i]:
                return False
        return True


class ItemsetTree(AbstractItemsetTree):
    def __init__(self):
        super().__init__()

    def buildTree(self, input_path):
        import time

        self.startTimestamp = int(time.time() * 1000)
        MemoryLogger.getInstance().reset()
        self.root = ItemsetTreeNode(None, 0)

        with open(input_path, "r", encoding="utf-8") as reader:
            for raw_line in reader:
                line = raw_line.strip()
                if not line or line[0] in "#%@":
                    continue
                itemset = [int(item) for item in line.split(" ")]
                self.construct(None, self.root, itemset)

        MemoryLogger.getInstance().checkMemory()
        self.endTimestamp = int(time.time() * 1000)

    def addTransaction(self, transaction):
        self.construct(None, self.root, list(transaction))

    def construct(self, parentOfR, r, s):
        sr = r.itemset

        if self.same(s, sr):
            r.support += 1
            return

        if self.ancestorOf(s, sr):
            newNode = ItemsetTreeNode(list(s), r.support + 1)
            newNode.childs.append(r)
            parentOfR.childs.remove(r)
            parentOfR.childs.append(newNode)
            return

        lca = self.getLargestCommonAncestor(s, sr)
        if lca is not None:
            newNode = ItemsetTreeNode(lca, r.support + 1)
            newNode.childs.append(r)
            parentOfR.childs.remove(r)
            parentOfR.childs.append(newNode)
            newNode2 = ItemsetTreeNode(list(s), 1)
            newNode.childs.append(newNode2)
            return

        indexLastItemOfR = 0 if sr is None else len(sr)
        r.support += 1

        for ci in list(r.childs):
            if self.same(s, ci.itemset):
                ci.support += 1
                return

            if self.ancestorOf(s, ci.itemset):
                newNode = ItemsetTreeNode(list(s), ci.support + 1)
                newNode.childs.append(ci)
                r.childs.remove(ci)
                r.childs.append(newNode)
                return

            if self.ancestorOf(ci.itemset, s):
                self.construct(r, ci, s)
                return

            if ci.itemset[indexLastItemOfR] == s[indexLastItemOfR]:
                ancestor = self.getLargestCommonAncestor(s, ci.itemset)
                newNode = ItemsetTreeNode(ancestor, ci.support + 1)
                r.childs.append(newNode)
                newNode.childs.append(ci)
                r.childs.remove(ci)
                newNode2 = ItemsetTreeNode(list(s), 1)
                newNode.childs.append(newNode2)
                return

        newNode = ItemsetTreeNode(list(s), 1)
        r.childs.append(newNode)

    def printStatisticsLines(self):
        lines = []
        lines.append("========== ITEMSET TREE CONSTRUCTION - STATS ============")
        lines.append(f" Tree construction time ~: {self.endTimestamp - self.startTimestamp} ms")
        lines.append(f" Max memory:{MemoryLogger.getInstance().getMaxMemory()}")
        self.nodeCount = 0
        self.totalItemCountInNodes = 0
        self.recursiveStats(self.root)
        lines.append(f" Node count: {self.nodeCount}")
        avg = self.totalItemCountInNodes / float(self.nodeCount)
        lines.append(f" Sum of items in all node: {self.totalItemCountInNodes} avg per node :{avg}")
        lines.append("=====================================")
        return lines

    def recursiveStats(self, root):
        if root is not None and root.itemset is not None:
            self.nodeCount += 1
            self.totalItemCountInNodes += len(root.itemset)
        for node in root.childs:
            self.recursiveStats(node)

    def printTreeLines(self):
        return self.toString().rstrip("\n").splitlines()

    def toString(self):
        return self.root.toString([], "").__str__()

    def printTree(self):
        print(self.toString(), end="")

    def getSupportOfItemset(self, s):
        return self.count(list(s), self.root)

    def count(self, s, root):
        count = 0
        for ci in root.childs:
            if ci.itemset[0] <= s[0]:
                if self.includedIn(s, ci.itemset):
                    count += ci.support
                elif ci.itemset[-1] < s[-1]:
                    count += self.count(s, ci)
        return count

    def includedIn(self, itemset1, itemset2):
        count = 0
        for item in itemset2:
            if item == itemset1[count]:
                count += 1
                if count == len(itemset1):
                    return True
        return False

    def getFrequentItemsetSubsuming(self, s):
        hash_table = HashTableIT(1000)
        seti = set(s)
        self.selectiveMining(list(s), seti, self.root, hash_table)
        return hash_table

    def selectiveMining(self, s, seti, t, hash_table):
        childrenSup = 0
        for ci in t.childs:
            childrenSup += ci.support
            if ci.itemset[0] <= s[0]:
                if self.includedIn(s, ci.itemset):
                    if len(ci.childs) == 0:
                        hash_table.put(s, ci.support)
                        self.recursiveAdd(s, seti, ci.itemset, ci.support, hash_table, 0)
                    else:
                        remainingSup = ci.support - self.selectiveMining(s, seti, ci, hash_table)
                        if remainingSup > 0:
                            hash_table.put(s, remainingSup)
                            self.recursiveAdd(s, seti, ci.itemset, remainingSup, hash_table, 0)
                elif ci.itemset[-1] < s[-1]:
                    self.selectiveMining(s, seti, ci, hash_table)
        return childrenSup

    def recursiveAdd(self, s, seti, ci, cisupport, hash_table, pos):
        if pos >= len(ci):
            return
        if ci[pos] not in seti:
            newS = [0] * (len(s) + 1)
            j = 0
            added = False
            for item in s:
                if added or item < ci[pos]:
                    newS[j] = item
                    j += 1
                else:
                    newS[j] = ci[pos]
                    j += 1
                    newS[j] = item
                    j += 1
                    added = True
            if j < len(s) + 1:
                newS[j] = ci[pos]
            hash_table.put(newS, cisupport)
            self.recursiveAdd(newS, seti, ci, cisupport, hash_table, pos + 1)
        self.recursiveAdd(s, seti, ci, cisupport, hash_table, pos + 1)


class ItemsetTreeNode:
    def __init__(self, itemset, support):
        self.itemset = itemset
        self.support = support
        self.childs = []

    def toString(self, buffer, space):
        buffer.append(space)
        if self.itemset is None:
            buffer.append("{}")
        else:
            buffer.append("[")
            for item in self.itemset:
                buffer.append(str(item))
                buffer.append(" ")
            buffer.append("]")
        buffer.append("   sup=")
        buffer.append(str(self.support))
        buffer.append("\n")
        for node in self.childs:
            node.toString(buffer, space + "  ")
        return "".join(buffer)

    def __str__(self):
        return self.toString([], "  ")


class ArraysAlgos:
    @staticmethod
    def intersectTwoSortedArrays(array1, array2):
        intersection = []
        pos1 = 0
        pos2 = 0
        while pos1 < len(array1) and pos2 < len(array2):
            if array1[pos1] < array2[pos2]:
                pos1 += 1
            elif array2[pos2] < array1[pos1]:
                pos2 += 1
            else:
                intersection.append(array1[pos1])
                pos1 += 1
                pos2 += 1
        return intersection


class AbstractItemset:
    def size(self):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def print(self):
        print(str(self), end="")

    def getAbsoluteSupport(self):
        raise NotImplementedError

    def getRelativeSupport(self, nbObject):
        raise NotImplementedError

    def contains(self, item):
        raise NotImplementedError


class AbstractOrderedItemset(AbstractItemset):
    def getLastItem(self):
        return self.get(self.size() - 1)

    def __str__(self):
        if self.size() == 0:
            return "EMPTYSET"
        result = []
        for i in range(self.size()):
            result.append(str(self.get(i)))
            result.append(" ")
        return "".join(result)

    def getRelativeSupport(self, nbObject):
        return float(self.getAbsoluteSupport()) / float(nbObject)

    def contains(self, item):
        for i in range(self.size()):
            if self.get(i) == item:
                return True
            if self.get(i) > item:
                return False
        return False


class Itemset(AbstractOrderedItemset):
    def __init__(self, items=None, support=0):
        self.itemset = [] if items is None else list(items)
        self.support = support

    def getItems(self):
        return self.itemset

    def getAbsoluteSupport(self):
        return self.support

    def size(self):
        return len(self.itemset)

    def get(self, position):
        return self.itemset[position]

    def setAbsoluteSupport(self, support):
        self.support = support

    def increaseTransactionCount(self):
        self.support += 1

    def cloneItemSetMinusOneItem(self, itemToRemove):
        return Itemset([item for item in self.itemset if item != itemToRemove])

    def cloneItemSetMinusAnItemset(self, itemsetToNotKeep):
        return Itemset([item for item in self.itemset if not itemsetToNotKeep.contains(item)])

    def intersection(self, itemset2):
        return Itemset(ArraysAlgos.intersectTwoSortedArrays(self.getItems(), itemset2.getItems()))


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


def fileToPath(filename):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def main():
    input_path = fileToPath("contextItemsetTree.txt")
    output_path = fileToPath("output_python.txt")
    output_lines = []

    def emit(line):
        print(line)
        output_lines.append(line)

    itemsetTree = ItemsetTree()
    itemsetTree.buildTree(input_path)

    for line in itemsetTree.printStatisticsLines():
        emit(line)
    emit("THIS IS THE TREE:")
    for line in itemsetTree.printTreeLines():
        emit(line)

    emit("THIS IS THE TREE AFTER ADDING A NEW TRANSACTION {4,5}:")
    itemsetTree.addTransaction([4, 5])
    for line in itemsetTree.printTreeLines():
        emit(line)

    emit("EXAMPLES QUERIES: FIND THE SUPPORT OF SOME ITEMSETS:")
    emit("the support of 1 2 3 is : " + str(itemsetTree.getSupportOfItemset([1, 2, 3])))
    emit("the support of 2 is : " + str(itemsetTree.getSupportOfItemset([2])))
    emit("the support of 2 4 is : " + str(itemsetTree.getSupportOfItemset([2, 4])))
    emit("the support of 1 2 is : " + str(itemsetTree.getSupportOfItemset([1, 2])))

    emit("EXAMPLE QUERY: FIND ALL ITEMSETS THAT SUBSUME {1 2}")
    result = itemsetTree.getFrequentItemsetSubsuming([1, 2])
    for lst in result.table:
        if lst is not None:
            for itemset in lst:
                emit("[" + str(itemset) + "]    supp:" + str(itemset.support))

    emit("EXAMPLE QUERY: FIND ALL ITEMSETS THAT SUBSUME {1} and minsup >= 2")
    minsup = 2
    result2 = itemsetTree.getFrequentItemsetSubsumingWithMinSup([1], minsup)
    for lst in result2.table:
        if lst is not None:
            for itemset in lst:
                emit("[" + str(itemset) + "]    supp:" + str(itemset.support))

    emit("EXAMPLE QUERY: FIND ALL ASSOCIATION RULE WITH AN ITEMSET {1} AS ANTECEDENT AND MINSUP >= 2 and minconf >= 0.1")
    rules = itemsetTree.generateRules([1], 2, 0.1)
    for rule in rules:
        emit(str(rule))

    with open(output_path, "w", encoding="utf-8", newline="") as writer:
        writer.write("\n".join(output_lines))
        writer.write("\n")


if __name__ == "__main__":
    main()
