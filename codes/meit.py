from dataclasses import dataclass
from typing import List, Optional, Set, Iterable


class ArraysAlgos:
    @staticmethod
    def cloneItemSetMinusOneItem(itemset: List[int], itemToRemove: int) -> List[int]:
        return [x for x in itemset if x != itemToRemove]

    @staticmethod
    def cloneItemSetMinusAnItemset(itemset: List[int], itemsetToNotKeep: List[int]) -> List[int]:
        import bisect
        # itemsetToNotKeep must be sorted
        res = []
        for x in itemset:
            i = bisect.bisect_left(itemsetToNotKeep, x)
            if i >= len(itemsetToNotKeep) or itemsetToNotKeep[i] != x:
                res.append(x)
        return res

    @staticmethod
    def allTheSameExceptLastItem(itemset1: List[int], itemset2: List[int]) -> bool:
        for i in range(len(itemset1) - 1):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    @staticmethod
    def concatenate(prefix: List[int], suffix: List[int]) -> List[int]:
        return list(prefix) + list(suffix)

    @staticmethod
    def intersectTwoSortedArrays(array1: List[int], array2: List[int]) -> List[int]:
        pos1 = pos2 = 0
        out = []
        while pos1 < len(array1) and pos2 < len(array2):
            a = array1[pos1]
            b = array2[pos2]
            if a < b:
                pos1 += 1
            elif b < a:
                pos2 += 1
            else:
                out.append(a)
                pos1 += 1
                pos2 += 1
        return out

    @staticmethod
    def containsOrEquals_list(itemset1: List[int], itemset2: List[int]) -> bool:
        # itemset2 subset of itemset1 (both sorted)
        j = 0
        for val2 in itemset2:
            found = False
            while j < len(itemset1):
                val1 = itemset1[j]
                if val1 == val2:
                    found = True
                    j += 1
                    break
                elif val1 > val2:
                    return False
                j += 1
            if not found:
                return False
        return True

    @staticmethod
    def containsLEX(itemset: Iterable[int], item: int) -> bool:
        for v in itemset:
            if v == item:
                return True
            if v > item:
                return False
        return False

    @staticmethod
    def sameAs(itemset1: List[int], itemsets2: List[int], posRemoved: int) -> int:
        j = 0
        for i in range(len(itemset1)):
            if j == posRemoved:
                j += 1
            if itemset1[i] == itemsets2[j]:
                j += 1
            elif itemset1[i] > itemsets2[j]:
                return 1
            else:
                return -1
        return 0

    @staticmethod
    def includedIn(itemset1: List[int], itemset2: List[int]) -> bool:
        count = 0
        for v in itemset2:
            if v == itemset1[count]:
                count += 1
                if count == len(itemset1):
                    return True
        return False

    @staticmethod
    def includedIn2(itemset1: List[int], itemset2: List[int], itemset2Length: int) -> bool:
        count = 0
        for i in range(itemset2Length):
            if itemset2[i] == itemset1[count]:
                count += 1
                if count == len(itemset1):
                    return True
        return False

    @staticmethod
    def includedIn3(itemset1: List[int], itemset1Length: int, itemset2: List[int]) -> bool:
        count = 0
        for v in itemset2:
            if v == itemset1[count]:
                count += 1
                if count == itemset1Length:
                    return True
        return False

    @staticmethod
    def contains(itemset: List[int], item: int) -> bool:
        for v in itemset:
            if v == item:
                return True
            elif v > item:
                return False
        return False


# =========================
# ca/pfv/spmf/patterns/AbstractItemset.java  (Python)
# ca/pfv/spmf/patterns/AbstractOrderedItemset.java  (Python)
# ca/pfv/spmf/patterns/itemset_array_integers_with_count/Itemset.java  (Python)
# =========================

@dataclass
class Itemset:
    itemset: List[int]
    support: int = 0

    def getItems(self): return self.itemset
    def getAbsoluteSupport(self): return self.support
    def size(self): return len(self.itemset)
    def get(self, position: int): return self.itemset[position]
    def setAbsoluteSupport(self, support: int): self.support = support
    def increaseTransactionCount(self): self.support += 1

    def cloneItemSetMinusOneItem(self, itemToRemove: int) -> "Itemset":
        return Itemset(ArraysAlgos.cloneItemSetMinusOneItem(self.itemset, itemToRemove))

    def cloneItemSetMinusAnItemset(self, itemsetToNotKeep: "Itemset") -> "Itemset":
        exclude = set(itemsetToNotKeep.itemset)
        return Itemset([x for x in self.itemset if x not in exclude])

    def intersection(self, itemset2: "Itemset") -> "Itemset":
        inter = ArraysAlgos.intersectTwoSortedArrays(self.itemset, itemset2.itemset)
        return Itemset(inter)

    def __hash__(self): return hash(tuple(self.itemset))


# =========================
# ca/pfv/spmf/algorithms/frequentpatterns/itemsettree/AssociationRuleIT.java  (Python)
# =========================

@dataclass
class AssociationRuleIT:
    support: int = 0
    confidence: float = 0.0
    itemset1: Optional[List[int]] = None
    itemset2: Optional[List[int]] = None

    def __str__(self) -> str:
        lhs = " ".join(str(i) for i in (self.itemset1 or [])) + " "
        rhs = " ".join(str(i) for i in (self.itemset2 or [])) + " "
        return f"[ {lhs}] ==> [{rhs}]  #SUP: {self.support}  #CONF:{self.confidence}\n"


# =========================
# ca/pfv/spmf/algorithms/frequentpatterns/itemsettree/HashTableIT.java  (Python)
# =========================

class HashTableIT:
    def __init__(self, size: int):
        self.table: List[Optional[List[Itemset]]] = [None] * size

    def hashCode(self, items: List[int]) -> int:
        hc = 0
        for i, v in enumerate(items):
            hc += v + (i * 10)
        if hc < 0:
            hc = -hc
        mod = len(items) if len(items) > 0 else 1
        # Java used (hash % items.length); map that into table range to avoid IndexError
        return (hc % mod) % len(self.table)

    def _same(self, a: Optional[List[int]], b: Optional[List[int]]) -> bool:
        if a is None or b is None:
            return False
        if len(a) != len(b):
            return False
        for i in range(len(a)):
            if a[i] != b[i]:
                return False
        return True

    def put(self, items: List[int], support: int) -> None:
        idx = self.hashCode(items)
        if self.table[idx] is None:
            self.table[idx] = []
        bucket = self.table[idx]
        for it in bucket:
            if self._same(items, it.itemset):
                it.support += support
                return
        bucket.append(Itemset(list(items), support))


# =========================
# ca/pfv/spmf/algorithms/frequentpatterns/itemsettree/ItemsetTreeNode.java  (Python)
# =========================

class ItemsetTreeNode:
    def __init__(self, itemset: Optional[List[int]], support: int):
        self.itemset = itemset
        self.support = support
        self.childs: Set["ItemsetTreeNode"] = set()

    def to_string(self, buffer: List[str], space: str) -> None:
        buffer.append(space)
        if self.itemset is None:
            buffer.append("{}")
        else:
            buffer.append("[" + " ".join(str(i) for i in self.itemset) + " ]")
        buffer.append("   sup=" + str(self.support) + "\n")
        for node in self.childs:
            node.to_string(buffer, space + "  ")

    def __str__(self):
        buf = []
        self.to_string(buf, "  ")
        return "".join(buf)


# =========================
# ca/pfv/spmf/tools/MemoryLogger.java  (Python)
# =========================

class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self):
        self.maxMemory = 0.0
        self.recordingMode = False
        self._writer = None

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def getMaxMemory(self) -> float:
        return self.maxMemory

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self) -> float:
        # Not measuring real process memory to keep this portable
        currentMemory = 0.0
        if currentMemory > self.maxMemory:
            self.maxMemory = currentMemory
        if self.recordingMode and self._writer:
            self._writer.write(f"{currentMemory}\n")
        return currentMemory

    def startRecordingMode(self, fileName: str):
        self.recordingMode = True
        self._writer = open(fileName, "w", encoding="utf-8")

    def stopRecordingMode(self):
        if self.recordingMode and self._writer:
            try:
                self._writer.close()
            finally:
                self.recordingMode = False
                self._writer = None


# =========================
# ca/pfv/spmf/algorithms/frequentpatterns/itemsettree/AbstractItemsetTree.java  (Python)
# =========================

class AbstractItemsetTree:
    def __init__(self):
        self.root: Optional[ItemsetTreeNode] = None
        self.nodeCount = 0
        self.totalItemCountInNodes = 0
        self.startTimestamp = 0
        self.endTimestamp = 0

    def getLargestCommonAncestor(self, itemset1: Optional[List[int]], itemset2: Optional[List[int]]) -> Optional[List[int]]:
        if itemset1 is None or itemset2 is None:
            return None
        minI = min(len(itemset1), len(itemset2))
        count = 0
        for i in range(minI):
            if itemset1[i] != itemset2[i]:
                break
            count += 1
        if count > 0 and count < minI:
            return list(itemset1[:count])
        else:
            return None

    def ancestorOf(self, itemset1: Optional[List[int]], itemset2: Optional[List[int]]) -> bool:
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

    def same(self, itemset1: Optional[List[int]], itemset2: Optional[List[int]]) -> bool:
        if itemset1 is None or itemset2 is None:
            return False
        if len(itemset1) != len(itemset2):
            return False
        for i in range(len(itemset1)):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    def getFrequentItemsetSubsuming_with_minsup(self, iset: List[int], minsup: int) -> HashTableIT:
        ht = self.getFrequentItemsetSubsuming(iset)
        for i, bucket in enumerate(ht.table):
            if bucket is None:
                continue
            ht.table[i] = [it for it in bucket if it.support >= minsup]
        return ht

    # To be implemented in subclass
    def getFrequentItemsetSubsuming(self, s: List[int]) -> HashTableIT:
        raise NotImplementedError

    def generateRules(self, s: List[int], minsup: int, minconf: float) -> List[AssociationRuleIT]:
        rules: List[AssociationRuleIT] = []
        seti = set(s)
        suppS = self.getSupportOfItemset(s)
        freq = self.getFrequentItemsetSubsuming_with_minsup(s, minsup)
        for bucket in freq.table:
            if not bucket:
                continue
            for c in bucket:
                if c.size() == len(s):
                    continue
                l = [item for item in c.itemset if item not in seti]
                suppC = self.getSupportOfItemset(c.itemset)
                conf = (suppC / suppS) if suppS > 0 else 0.0
                if conf >= minconf:
                    rules.append(AssociationRuleIT(support=suppC, confidence=conf, itemset1=list(s), itemset2=l))
        return rules

    # To be implemented in subclass
    def getSupportOfItemset(self, s: List[int]) -> int:
        raise NotImplementedError


# =========================
# ca/pfv/spmf/algorithms/frequentpatterns/itemsettree/MemoryEfficientItemsetTree.java  (Python)
# =========================

class MemoryEfficientItemsetTree(AbstractItemsetTree):
    def __init__(self):
        super().__init__()
        self.sumBranchesLength = 0
        self.totalNumberOfBranches = 0
        self.root = ItemsetTreeNode(None, 0)

    def buildTree(self, input_path: str):
        import time
        self.startTimestamp = int(time.time() * 1000)
        MemoryLogger.getInstance().reset()
        self.root = ItemsetTreeNode(None, 0)

        with open(input_path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                parts = line.split()
                itemset = [int(x) for x in parts]
                self.construct(None, self.root, itemset, None)

        MemoryLogger.getInstance().checkMemory()
        self.endTimestamp = int(time.time() * 1000)

    def addTransaction(self, transaction: List[int]):
        self.construct(None, self.root, transaction, None)

    # --- helpers mirroring Java private methods ---

    def append(self, a1: Optional[List[int]], a2: Optional[List[int]]) -> Optional[List[int]]:
        if a1 is None:
            return None if a2 is None else list(a2)
        if a2 is None:
            return list(a1)
        return list(a1) + list(a2)

    def same_with_prefix(self, itemset1: Optional[List[int]], prefix: Optional[List[int]], itemset2: Optional[List[int]]) -> bool:
        if prefix is None:
            return self.same(itemset1, itemset2)
        if itemset1 is None or itemset2 is None:
            return False
        if len(itemset1) != len(prefix) + len(itemset2):
            return False
        # compare prefix
        for i in range(len(prefix)):
            if itemset1[i] != prefix[i]:
                return False
        # compare remainder
        base = len(prefix)
        for j in range(len(itemset2)):
            if itemset1[base + j] != itemset2[j]:
                return False
        return True

    def copyItemsetWithoutItemsFromArrays(self, r: List[int], prefix: Optional[List[int]], s: Optional[List[int]]) -> List[int]:
        rprime = []
        for rv in r:
            if prefix is not None:
                skip = False
                for pv in prefix:
                    if pv == rv:
                        skip = True
                        break
                    elif pv > rv:
                        break
                if skip:
                    continue
            if s is not None:
                skip = False
                for sv in s:
                    if sv == rv:
                        skip = True
                        break
                    elif sv > rv:
                        break
                if skip:
                    continue
            rprime.append(rv)
        return rprime

    def copyItemsetWithoutItemsFrom(self, itemset1: List[int], itemset2: Optional[List[int]]) -> List[int]:
        if itemset2 is None:
            return list(itemset1)
        out = []
        for v1 in itemset1:
            in2 = False
            for v2 in itemset2:
                if v2 == v1:
                    in2 = True
                    break
                elif v2 > v1:
                    break
            if not in2:
                out.append(v1)
        return out

    # --- tree construction ---

    def construct(self, parentOfR: Optional[ItemsetTreeNode], r: ItemsetTreeNode, s: List[int], prefix: Optional[List[int]]):
        if self.same_with_prefix(s, prefix, r.itemset):
            r.support += 1
            return

        rprefix = self.append(prefix, r.itemset)

        if self.ancestorOf(s, rprefix):
            sprime = self.copyItemsetWithoutItemsFrom(s, prefix)
            rprime = self.copyItemsetWithoutItemsFrom(rprefix or [], sprime)
            newNodeS = ItemsetTreeNode(sprime, r.support + 1)
            newNodeS.childs.add(r)
            if parentOfR is not None:
                if r in parentOfR.childs:
                    parentOfR.childs.remove(r)
                parentOfR.childs.add(newNodeS)
            r.itemset = rprime
            return

        l = self.getLargestCommonAncestor(s, rprefix)
        if l is not None:
            sprime = self.copyItemsetWithoutItemsFrom(s, l)
            rprime = self.copyItemsetWithoutItemsFrom(r.itemset or [], l)
            newNode = ItemsetTreeNode(l, r.support + 1)
            newNode.childs.add(r)
            if parentOfR is not None:
                if r in parentOfR.childs:
                    parentOfR.childs.remove(r)
                parentOfR.childs.add(newNode)
            r.itemset = rprime
            newNode2 = ItemsetTreeNode(sprime, 1)
            newNode.childs.add(newNode2)
            return

        indexLastItemOfR = 0 if rprefix is None else len(rprefix)
        r.support += 1

        for ci in list(r.childs):
            ciprefix = self.append(rprefix, ci.itemset)

            if self.same(s, ciprefix):
                ci.support += 1
                return

            if self.ancestorOf(s, ciprefix):
                sprime = self.copyItemsetWithoutItemsFrom(s, rprefix)
                ciprime = self.copyItemsetWithoutItemsFrom(ci.itemset or [], s)
                newNode = ItemsetTreeNode(sprime, ci.support + 1)
                newNode.childs.add(ci)
                if ci in r.childs:
                    r.childs.remove(ci)
                r.childs.add(newNode)
                ci.itemset = ciprime
                return

            if self.ancestorOf(ciprefix, s):
                self.construct(r, ci, s, rprefix)
                return

            if ciprefix and indexLastItemOfR < len(ciprefix) and indexLastItemOfR < len(s) and ciprefix[indexLastItemOfR] == s[indexLastItemOfR]:
                ancestor = self.getLargestCommonAncestor(s, ciprefix)
                if ancestor is not None:
                    ancestorprime = self.copyItemsetWithoutItemsFrom(ancestor, rprefix)
                    newNode = ItemsetTreeNode(ancestorprime, ci.support + 1)
                    r.childs.add(newNode)
                    ci.itemset = self.copyItemsetWithoutItemsFrom(ci.itemset or [], ancestorprime)
                    newNode.childs.add(ci)
                    if ci in r.childs:
                        r.childs.remove(ci)
                    sprime = self.copyItemsetWithoutItemsFromArrays(s, ancestorprime, rprefix)
                    newNode2 = ItemsetTreeNode(sprime, 1)
                    newNode.childs.add(newNode2)
                    return

        sprime = self.copyItemsetWithoutItemsFrom(s, rprefix)
        newNode = ItemsetTreeNode(sprime, 1)
        r.childs.add(newNode)

    # --- stats / printing ---

    def recursiveStats(self, root: ItemsetTreeNode, length: int):
        if root is not None and root.itemset is not None:
            self.nodeCount += 1
            self.totalItemCountInNodes += len(root.itemset)
        for node in root.childs:
            self.recursiveStats(node, length + 1)
        if len(root.childs) == 0:
            self.sumBranchesLength += length
            self.totalNumberOfBranches += 1

    def printStatistics(self):
        import gc
        gc.collect()
        print("========== MEMORY EFFICIENT ITEMSET TREE CONSTRUCTION - STATS ============")
        print(f" Tree construction time ~: {self.endTimestamp - self.startTimestamp} ms")
        print(f" Max memory:{MemoryLogger.getInstance().getMaxMemory()}")
        self.nodeCount = 0
        self.totalItemCountInNodes = 0
        self.sumBranchesLength = 0
        self.totalNumberOfBranches = 0
        self.recursiveStats(self.root, 1)
        print(" Node count: " + str(self.nodeCount))
        avg = (self.totalItemCountInNodes / float(self.nodeCount)) if self.nodeCount > 0 else 0.0
        print(" Sum of items in all node: " + str(self.totalItemCountInNodes) + " avg per node :" + str(avg))
        print("=====================================")

    def printTree(self):
        buf: List[str] = []
        self.root.to_string(buf, "")
        print("".join(buf))

    def __str__(self):
        buf: List[str] = []
        self.root.to_string(buf, "")
        return "".join(buf)

    # --- support / mining ---

    def getSupportOfItemset(self, s: List[int]) -> int:
        return self._count(s, self.root, [])

    def _count(self, s: List[int], root: ItemsetTreeNode, prefix: List[int]) -> int:
        count = 0
        for ci in root.childs:
            ciprefix = self.append(prefix, ci.itemset) or []
            if ciprefix and ciprefix[0] <= s[0]:
                if ArraysAlgos.includedIn(s, ciprefix):
                    count += ci.support
                elif ciprefix[-1] < s[-1]:
                    count += self._count(s, ci, ciprefix)
        return count

    def getFrequentItemsetSubsuming(self, s: List[int]) -> HashTableIT:
        hash_table = HashTableIT(1000)
        seti = set(s)
        self._selectiveMining(s, seti, self.root, hash_table, None)
        return hash_table

    def _selectiveMining(self, s: List[int], seti: set, t: ItemsetTreeNode, hash_table: HashTableIT, prefix: Optional[List[int]]) -> int:
        childrenSup = 0
        for ci in t.childs:
            childrenSup += ci.support
            ciprefix = self.append(prefix, ci.itemset) or []
            if ciprefix and ciprefix[0] <= s[0]:
                if ArraysAlgos.includedIn(s, ciprefix):
                    if len(ci.childs) == 0:
                        hash_table.put(list(s), ci.support)
                        self._recursiveAdd(s, seti, ciprefix, ci.support, hash_table, 0)
                    else:
                        remainingSup = ci.support - self._selectiveMining(s, seti, ci, hash_table, ciprefix)
                        if remainingSup > 0:
                            hash_table.put(list(s), remainingSup)
                            self._recursiveAdd(s, seti, ciprefix, remainingSup, hash_table, 0)
                elif ciprefix[-1] < s[-1]:
                    self._selectiveMining(s, seti, ci, hash_table, ciprefix)
        return childrenSup

    def _recursiveAdd(self, s: List[int], seti: set, ci: List[int], cisupport: int, hash_table: HashTableIT, pos: int):
        if pos >= len(ci):
            return
        if ci[pos] not in seti:
            # Insert ci[pos] into s keeping lexicographic order
            newS = []
            added = False
            for item in s:
                if added or item < ci[pos]:
                    newS.append(item)
                else:
                    newS.append(ci[pos])
                    newS.append(item)
                    added = True
            if len(newS) < len(s) + 1:
                newS.append(ci[pos])
            hash_table.put(newS, cisupport)
            self._recursiveAdd(newS, seti, ci, cisupport, hash_table, pos + 1)
        self._recursiveAdd(s, seti, ci, cisupport, hash_table, pos + 1)


# =========================
# ca/pfv/spmf/test/MainTestMemoryEfficientItemsetTree.java  (Python)
# =========================

def _ensure_input_file_exists(path: str):
    """Create contextItemsetTree.txt with provided content if it doesn't exist."""
    import os
    if not os.path.exists(path):
        data = """1 4
2 5
1 2 3 4 5
1 2 4
2 5
2 4
"""
        with open(path, "w", encoding="utf-8") as f:
            f.write(data)


def main():
    import os
    input_path = os.path.join(os.getcwd(), "contextItemsetTree.txt")
    _ensure_input_file_exists(input_path)

    # Build the itemset tree
    itemsetTree = MemoryEfficientItemsetTree()
    itemsetTree.buildTree(input_path)

    # Stats and tree
    itemsetTree.printStatistics()
    print("THIS IS THE TREE:")
    itemsetTree.printTree()

    # Serialize-size equivalent (rough estimate omitted here)
    # Add a transaction {4,5}
    print("THIS IS THE TREE AFTER ADDING A NEW TRANSACTION {4,5}:")
    itemsetTree.addTransaction([4, 5])
    itemsetTree.printTree()

    # Example queries
    print("EXAMPLES QUERIES: FIND THE SUPPORT OF SOME ITEMSETS:")
    print("the support of 1 2 3 is :", itemsetTree.getSupportOfItemset([1, 2, 3]))
    print("the support of 2 is :", itemsetTree.getSupportOfItemset([2]))
    print("the support of 2 4 is :", itemsetTree.getSupportOfItemset([2, 4]))
    print("the support of 1 2 is :", itemsetTree.getSupportOfItemset([1, 2]))

    # Example: all itemsets that subsume {1 2}
    print("EXAMPLE QUERY: FIND ALL ITEMSETS THAT SUBSUME {1 2}")
    result = itemsetTree.getFrequentItemsetSubsuming([1, 2])
    for bucket in result.table:
        if bucket:
            for it in bucket:
                print(f"[ {' '.join(map(str, it.itemset))} ]    supp:{it.support}")

    # Example: get all itemsets that subsume {1} and minsup >= 
    print("EXAMPLE QUERY: FIND ALL ITEMSETS THAT SUBSUME {1} and minsup >= 3")
    minsup = 3
    result2 = itemsetTree.getFrequentItemsetSubsuming_with_minsup([3], minsup)
    for bucket in result2.table:
        if bucket:
            for it in bucket:
                print(f"[ {' '.join(map(str, it.itemset))} ]    supp:{it.support}")

    # Example: rules with antecedent {1}, minsup >= 3, minconf >= 0.1
    print("EXAMPLE QUERY: FIND ALL ASSOCIATION RULE WITH AN ITEMSET {1} AS ANTECEDENT AND MINSUP >= 3 and minconf >= 0.1")
    rules = itemsetTree.generateRules([1], minsup=3, minconf=0.1)
    for r in rules:
        print(r, end="")


if __name__ == "__main__":
    import sys
    output = "output_python.txt"
    with open(output, "w", encoding="utf-8") as out:
        sys_stdout = sys.stdout
        sys.stdout = out
        try:
            main()
        finally:
            sys.stdout = sys_stdout
