import math
import os
import time
import tracemalloc
def _java_like_float(value):
    if isinstance(value, int):
        return f"{float(value):.1f}"
    v = float(value)
    if abs(v - round(v)) < 1e-12:
        return f"{float(round(v)):.1f}"
    s = repr(v)
    if "e" in s or "E" in s:
        s = format(v, ".16f").rstrip("0").rstrip(".")
    if "." in s:
        frac = s.split(".", 1)[1]
        if len(frac) > 15 and s.endswith("5") and len(s) >= 2 and s[-2] == "9":
            s = s[:-1]
    return s


class MemoryLogger:
    _instance = None

    def __init__(self):
        self.max_memory = 0.0

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def getMaxMemory(self):
        return self.max_memory

    def reset(self):
        self.max_memory = 0.0

    def checkMemory(self):
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, _ = tracemalloc.get_traced_memory()
        current_mb = current / 1024.0 / 1024.0
        if current_mb > self.max_memory:
            self.max_memory = current_mb
        return current_mb


class BitSet:
    def __init__(self):
        self.bits = set()

    def set(self, idx):
        self.bits.add(int(idx))

    def get(self, idx):
        return int(idx) in self.bits

    def clear(self):
        self.bits.clear()

    def cardinality(self):
        return len(self.bits)

    def clone(self):
        b = BitSet()
        b.bits = set(self.bits)
        return b

    def and_op(self, other):
        self.bits.intersection_update(other.bits)

    def length(self):
        return 0 if not self.bits else (max(self.bits) + 1)

    def iter_set_bits(self):
        for x in sorted(self.bits):
            yield x


class Abstraction_Generic:
    def compare_to(self, other):
        raise NotImplementedError

    def toStringToFile(self):
        raise NotImplementedError


class Abstraction_Qualitative(Abstraction_Generic):
    _pool = {}

    def __init__(self, equal_relation):
        self.equal_relation = bool(equal_relation)

    @staticmethod
    def create(equal_relation):
        key = bool(equal_relation)
        if not Abstraction_Qualitative._pool:
            Abstraction_Qualitative._pool[True] = Abstraction_Qualitative(True)
            Abstraction_Qualitative._pool[False] = Abstraction_Qualitative(False)
        return Abstraction_Qualitative._pool[key]

    def hasEqualRelation(self):
        return self.equal_relation

    def compare_to(self, other):
        if self.equal_relation == other.equal_relation:
            return 0
        return -1 if not self.equal_relation else 1

    def __eq__(self, other):
        return isinstance(other, Abstraction_Qualitative) and self.equal_relation == other.equal_relation

    def __hash__(self):
        return 17 * 7 + (1 if self.equal_relation else 0)

    def __str__(self):
        return "" if self.equal_relation else " ->"

    def toStringToFile(self):
        return "" if self.equal_relation else " -1"

    @staticmethod
    def clear():
        pass


class Item:
    def __init__(self, item_id):
        self.id = item_id

    def getId(self):
        return self.id

    def __str__(self):
        return str(self.id)

    def __eq__(self, other):
        return isinstance(other, Item) and self.id == other.id

    def __hash__(self):
        return 17 * 7 + hash(self.id)

    def compareTo(self, other):
        if self.id < other.id:
            return 1
        if self.id > other.id:
            return -1
        return 0


class Itemset:
    def __init__(self):
        self.items = []
        self.timestamp = 0

    def addItem(self, *args):
        if len(args) == 1:
            self.items.append(args[0])
        else:
            idx, value = args
            if idx < len(self.items):
                self.items[idx] = value
            else:
                self.items.append(value)

    def removeItem(self, value):
        if isinstance(value, int):
            return self.items.pop(value)
        self.items.remove(value)
        return True

    def getItems(self):
        return self.items

    def containItem(self, value):
        return any(it == value for it in self.items)

    def get(self, idx):
        return self.items[idx]

    def cloneItemset(self):
        c = Itemset()
        c.timestamp = self.timestamp
        c.items.extend(self.items)
        return c

    def getTimestamp(self):
        return self.timestamp

    def setTimestamp(self, ts):
        self.timestamp = int(ts)

    def size(self):
        return len(self.items)

    def binarySearch(self, item):
        for i, it in enumerate(self.items):
            if it == item:
                return i
        return -1


class Sequence:
    def __init__(self, seq_id):
        self.id = seq_id
        self.itemsets = []
        self.numberOfItems = 0

    def addItemset(self, itemset):
        self.itemsets.append(itemset)
        self.numberOfItems += itemset.size()

    def addItem(self, index_itemset, item=None):
        if item is None:
            self.itemsets[self.size() - 1].addItem(index_itemset)
        else:
            self.itemsets[index_itemset].addItem(item)
        self.numberOfItems += 1

    def remove(self, index_itemset, index_item=None):
        if index_item is None:
            itemset = self.itemsets.pop(index_itemset)
            self.numberOfItems -= itemset.size()
            return itemset
        self.numberOfItems -= 1
        return self.itemsets[index_itemset].removeItem(index_item)

    def getId(self):
        return self.id

    def get(self, idx):
        return self.itemsets[idx]

    def size(self):
        return len(self.itemsets)

    def getLength(self):
        return self.numberOfItems

    def getItemsets(self):
        return self.itemsets

    def searchForTheFirstAppearance(self, itemset_index, item_index, item):
        if itemset_index < self.size():
            for i in range(itemset_index, self.size()):
                pos = self.itemsets[i].binarySearch(item)
                begin = item_index if i == itemset_index else 0
                if pos >= begin:
                    return [i, pos]
        return None

    def SearchForItemAtTheSameItemset(self, item, itemset_index, item_index):
        if itemset_index < self.size():
            pos = self.itemsets[itemset_index].binarySearch(item)
            if pos >= item_index:
                return [itemset_index, pos]
        return None


class ItemAbstractionPair:
    def __init__(self, item, abstraction):
        self.item = item
        self.abstraction = abstraction

    def getItem(self):
        return self.item

    def getAbstraction(self):
        return self.abstraction

    def compareTo(self, other):
        cmp_item = self.item.compareTo(other.item)
        if cmp_item == 0:
            return self.abstraction.compare_to(other.abstraction)
        return cmp_item

    def __eq__(self, other):
        return isinstance(other, ItemAbstractionPair) and self.item == other.item and self.abstraction == other.abstraction

    def __hash__(self):
        return 53 * (53 * 7 + hash(self.item)) + hash(self.abstraction)

    def __str__(self):
        if isinstance(self.abstraction, Abstraction_Qualitative):
            return str(self.abstraction) + " " + str(self.item)
        return str(self.item) + str(self.abstraction) + " "

    def toStringToFile(self):
        if isinstance(self.abstraction, Abstraction_Qualitative):
            return self.abstraction.toStringToFile() + " " + str(self.item)
        return str(self.item) + str(self.abstraction) + " "


class ItemAbstractionPairCreator:
    instance = None

    @staticmethod
    def clear():
        ItemAbstractionPairCreator.instance = None

    @staticmethod
    def getInstance():
        if ItemAbstractionPairCreator.instance is None:
            ItemAbstractionPairCreator.instance = ItemAbstractionPairCreator()
        return ItemAbstractionPairCreator.instance

    def getItemAbstractionPair(self, item, abstraction):
        return ItemAbstractionPair(item, abstraction)


class Pattern:
    def __init__(self, elements=None):
        self.elements = [] if elements is None else elements
        self.appearingIn = BitSet()
        self.total_length_constraint_sup = 0.0
        self.total_discrete_constraint_sup = 0.0
        self.total_vality_constraint_sup = 0.0
        self.total_three_constraint_integration_sup = 0.0

    def clonePattern(self):
        return Pattern(list(self.elements))

    def add(self, pair):
        self.elements.append(pair)

    def size(self):
        return len(self.elements)

    def getElements(self):
        return self.elements

    def getIthElement(self, i):
        return self.elements[i]

    def getLastElement(self):
        return self.elements[-1] if self.elements else None

    def addAppearance(self, sid):
        self.appearingIn.set(sid)

    def getAppearingIn(self):
        return self.appearingIn

    def getSupport(self):
        return self.appearingIn.cardinality()

    def compareTo(self, other):
        if len(self.elements) >= len(other.elements):
            bigger, smaller = self.elements, other.elements
        else:
            bigger, smaller = other.elements, self.elements
        for i in range(len(smaller)):
            c = smaller[i].compareTo(bigger[i])
            if c != 0:
                return c
        if len(bigger) == len(smaller):
            return 0
        return -1 if len(self.elements) < len(other.elements) else 1

    def __lt__(self, other):
        return self.compareTo(other) < 0

    def __eq__(self, other):
        return isinstance(other, Pattern) and self.compareTo(other) == 0

    def __hash__(self):
        return 67 * 5 + hash(tuple(self.elements))

    def toStringToFile(self, outputSequenceIdentifiers):
        out = []
        for i in range(len(self.elements)):
            if i == len(self.elements) - 1:
                out.append(str(self.elements[i].getItem()) if i == 0 else self.elements[i].toStringToFile())
                out.append(" -1")
            elif i == 0:
                out.append(str(self.elements[i].getItem()))
            else:
                out.append(self.elements[i].toStringToFile())
        out.append(" #LENGTH: ")
        out.append(_java_like_float(self.total_length_constraint_sup))
        out.append(" #DISCRETE: ")
        out.append(_java_like_float(self.total_discrete_constraint_sup))
        out.append(" #VALIDITY: ")
        out.append(_java_like_float(self.total_vality_constraint_sup))
        out.append(" #INTEGRATION: ")
        out.append(_java_like_float(self.total_three_constraint_integration_sup))
        if outputSequenceIdentifiers:
            out.append(" #SID: ")
            for sid in self.appearingIn.iter_set_bits():
                out.append(str(sid))
                out.append(" ")
        return "".join(out)


class PatternCreator:
    instance = None

    @staticmethod
    def getInstance():
        if PatternCreator.instance is None:
            PatternCreator.instance = PatternCreator()
        return PatternCreator.instance

    @staticmethod
    def sclear():
        pass

    def createPattern(self, elements=None):
        if elements is None:
            return Pattern([])
        if isinstance(elements, ItemAbstractionPair):
            return Pattern([elements])
        return Pattern(elements)


class ItemFactory:
    def __init__(self):
        self.pool = {}

    def getItem(self, key):
        if key not in self.pool:
            self.pool[key] = Item(key)
        return self.pool[key]

class SequenceDatabase:
    primarysequences = []

    def __init__(self, abstractionCreator):
        self.abstractionCreator = abstractionCreator
        self._frequentItemsMap = {}
        self.sequences = []
        self.itemFactory = ItemFactory()
        self.patternCreator = PatternCreator.getInstance()

    def loadFile(self, path, minSupportAbsolute, alpha, beta, gamma):
        max_sequence_length = 398
        k1 = 13.2667
        k3 = 0.14

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line[0] in ["#", "%", "@"]:
                    continue
                self.addSequence(line.split(" "))

        minSupRelative = int(math.ceil(minSupportAbsolute * len(self.sequences)))
        items_to_remove = set()
        for item, pattern in list(self._frequentItemsMap.items()):
            local_total_len = 0.0
            pattern.total_length_constraint_sup = 0.0
            pattern.total_discrete_constraint_sup = pattern.getSupport()
            for m in pattern.getAppearingIn().iter_set_bits():
                each_len = math.exp((-1.0 * k1) * (1.0 / max_sequence_length) * SequenceDatabase.primarysequences[m].getLength())
                local_total_len += each_len
                low_num = 0
                seq = self.sequences[m]
                for k in range(seq.size()):
                    if seq.get(k).containItem(item):
                        ts = str(int(seq.get(k).getTimestamp()))
                        if int(ts[-1:]) < 1:
                            low_num += 1
                pattern.total_vality_constraint_sup += math.exp(-1.0 * k3 * low_num)
            pattern.total_length_constraint_sup = local_total_len
            pattern.total_discrete_constraint_sup = pattern.getSupport()
            pattern.total_three_constraint_integration_sup = (
                alpha * pattern.total_length_constraint_sup
                + beta * pattern.total_discrete_constraint_sup
                + gamma * pattern.total_vality_constraint_sup
            )
            if pattern.total_three_constraint_integration_sup < minSupRelative:
                items_to_remove.add(item)

        for item in items_to_remove:
            self._frequentItemsMap.pop(item, None)

        self.shrinkDatabase(set(self._frequentItemsMap.keys()))

    def addSequence(self, tokens):
        pair_creator = ItemAbstractionPairCreator.getInstance()
        sequence = Sequence(len(self.sequences))
        primary = Sequence(len(self.sequences))
        itemset = Itemset()

        for token in tokens:
            token = token.strip()
            if not token:
                continue
            if token[0] == "<":
                itemset.setTimestamp(int(token[1:-1]))
            elif token == "-1":
                next_time = itemset.getTimestamp() + 1
                sequence.addItemset(itemset)
                primary.addItemset(itemset)
                itemset = Itemset()
                itemset.setTimestamp(next_time)
            elif token == "-2":
                self.sequences.append(sequence)
                SequenceDatabase.primarysequences.append(primary)
            else:
                item = self.itemFactory.getItem(int(token))
                pattern = self._frequentItemsMap.get(item)
                if pattern is None:
                    pattern = self.patternCreator.createPattern(
                        pair_creator.getItemAbstractionPair(item, self.abstractionCreator.CreateDefaultAbstraction())
                    )
                    self._frequentItemsMap[item] = pattern
                pattern.addAppearance(sequence.getId())
                itemset.addItem(item)

    def shrinkDatabase(self, frequent_items):
        for sequence in self.sequences:
            i = 0
            while i < sequence.size():
                itemset = sequence.get(i)
                j = 0
                while j < itemset.size():
                    item = itemset.get(j)
                    if item not in frequent_items:
                        sequence.remove(i, j)
                        j -= 1
                    j += 1
                if itemset.size() == 0:
                    sequence.remove(i)
                    i -= 1
                i += 1

    def frequentItems(self):
        out = list(self._frequentItemsMap.values())
        out.sort()
        return out

    def getFrequentItems(self):
        return self._frequentItemsMap

    def getSequences(self):
        return self.sequences

    def size(self):
        return len(self.sequences)


class CandidateInSequenceFinder:
    def __init__(self, creator):
        self.creator = creator
        self.present = False

    def isCandidatePresentInTheSequence_qualitative(self, candidate, sequence, k, length, position):
        pair = candidate.getIthElement(length)
        itemPair = pair.getItem()
        absPair = pair.getAbstraction()
        prevAbs = candidate.getIthElement(length - 1).getAbstraction() if length > 0 else None
        cancelled = False

        while position[length][0] < sequence.size():
            if length == 0:
                pos = sequence.searchForTheFirstAppearance(position[length][0], position[length][1], itemPair)
            else:
                pos = self.creator.findPositionOfItemInSequence(
                    sequence,
                    itemPair,
                    absPair,
                    prevAbs,
                    position[length][0],
                    position[length][1],
                    position[length - 1][0],
                    position[length - 1][1],
                )
            if pos is not None:
                position[length] = pos
                if length + 1 < k:
                    position[length + 1] = self.increasePosition(sequence, position[length])
                    self.isCandidatePresentInTheSequence_qualitative(candidate, sequence, k, length + 1, position)
                    if self.present:
                        return
                else:
                    self.present = True
                    return
            else:
                if length > 0:
                    position[length - 1] = self.increaseItemset(position[length - 1])
                cancelled = True
                break

        if length > 0 and not cancelled:
            position[length - 1] = self.increaseItemset(position[length - 1])

    def isPresent(self):
        return self.present

    def increasePosition(self, sequence, pos):
        if pos[1] < sequence.get(pos[0]).size() - 1:
            return [pos[0], pos[1] + 1]
        return [pos[0] + 1, 0]

    def increaseItemset(self, pos):
        return [pos[0] + 1, 0]


class CandidateGeneration:
    def generateCandidates(self, frequentSet, abstractionCreator, indexationMap, k, minSupportAbsolute):
        frequentList = list(frequentSet)
        candidateSet = []

        if k == 2:
            out = []
            for i in range(len(frequentList)):
                for j in range(i, len(frequentList)):
                    out.extend(abstractionCreator.generateSize2Candidates(abstractionCreator, frequentList[i], frequentList[j]))
            return out if out else None

        previous_item = None
        matching = None
        for p1 in frequentList:
            current_item = p1.getIthElement(1).getItem()
            if previous_item is None or current_item != previous_item:
                matching = indexationMap.get(current_item)
                previous_item = current_item
            if matching is not None:
                for p2 in matching:
                    c = abstractionCreator.generateCandidates(abstractionCreator, p1, p2, minSupportAbsolute)
                    if c is not None:
                        candidateSet.append(c)

        if not candidateSet:
            return None

        pruned = []
        for cand in candidateSet:
            infrequent = False
            for i in range(cand.size()):
                sub = abstractionCreator.getSubpattern(cand, i)
                if sub not in frequentSet:
                    infrequent = True
                    break
            if not infrequent:
                pruned.append(cand)
        return pruned if pruned else None

class SupportCounting:
    def __init__(self, database, creator, alpha, beta, gamma):
        self.database = database
        self.abstractionCreator = creator
        self.indexationMap = {}
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma

    def countSupport(self, candidateSet, k, minSupportAbsolute):
        k1 = 13.2667
        k2 = 0.6
        k3 = 0.14
        max_min = 547
        max_sequence_length = 398

        self.indexationMap.clear()
        for sequence in self.database.getSequences():
            for candidate in candidateSet:
                position = [[0, 0] for _ in range(k)]
                finder = CandidateInSequenceFinder(self.abstractionCreator)
                self.abstractionCreator.isCandidateInSequence(finder, candidate, sequence, k, 0, position)
                if finder.isPresent():
                    timelist = []
                    flags = []
                    candidate.addAppearance(sequence.getId())
                    for pos in position:
                        ts = str(int(self.database.getSequences()[sequence.getId()].get(pos[0]).getTimestamp()))
                        timelist.append((float(ts[:-1]) - 1.0) / max_min)
                        flags.append(int(ts[-1:]))

                    avg = sum(timelist) / len(timelist) if timelist else 0.0
                    discrete_q = sum((x - avg) ** 2 for x in timelist)
                    candidate.total_discrete_constraint_sup += math.exp(-1.0 / k2 * discrete_q)

                    low_num = sum(1 for f in flags if f < 1)
                    candidate.total_vality_constraint_sup += math.exp(-1.0 * k3 * low_num)
                    candidate.addAppearance(sequence.getId())

        result = set()
        for candidate in candidateSet:
            total_len = 0.0
            for i in range(candidate.getAppearingIn().length()):
                if candidate.getAppearingIn().get(i):
                    total_len += math.exp((-1.0 * k1) * (1.0 / max_sequence_length) * SequenceDatabase.primarysequences[i].getLength())
            candidate.total_length_constraint_sup = total_len
            candidate.total_three_constraint_integration_sup = (
                self.alpha * candidate.total_length_constraint_sup
                + self.beta * candidate.total_discrete_constraint_sup
                + self.gamma * candidate.total_vality_constraint_sup
            )
            if candidate.total_three_constraint_integration_sup >= minSupportAbsolute:
                result.add(candidate)
                self.putInIndexationMap(candidate)

        return result

    def putInIndexationMap(self, pattern):
        first_item = pattern.getIthElement(0).getItem()
        if first_item not in self.indexationMap:
            self.indexationMap[first_item] = set()
        self.indexationMap[first_item].add(pattern)

    def getIndexationMap(self):
        return self.indexationMap


class Sequences:
    def __init__(self, name):
        self.levels = [[]]
        self.numberOfFrequentSequences = 0
        self.name = name

    def addSequence(self, sequence, level_index):
        while len(self.levels) <= level_index:
            self.levels.append([])
        self.levels[level_index].append(sequence)
        self.numberOfFrequentSequences += 1

    def addSequences(self, sequences, level_index):
        for p in sequences:
            self.addSequence(p, level_index)

    def getLevel(self, i):
        return self.levels[i]

    def getLevelCount(self):
        return len(self.levels) - 1

    def sort(self):
        for level in self.levels:
            level.sort()

    def delete(self, i):
        self.numberOfFrequentSequences -= len(self.levels[i])
        self.levels[i].clear()

    def toStringToFile(self, outputSequenceIdentifiers):
        out = []
        for level in self.levels:
            for seq in level:
                out.append(seq.toStringToFile(outputSequenceIdentifiers))
                out.append("\n")
        return "".join(out)


class AbstractionCreator_Qualitative:
    instance = None

    @staticmethod
    def getInstance():
        if AbstractionCreator_Qualitative.instance is None:
            AbstractionCreator_Qualitative.instance = AbstractionCreator_Qualitative()
        return AbstractionCreator_Qualitative.instance

    @staticmethod
    def sclear():
        AbstractionCreator_Qualitative.instance = None

    def CreateDefaultAbstraction(self):
        return Abstraction_Qualitative.create(False)

    def createAbstraction(self, *args):
        if len(args) == 1:
            return Abstraction_Qualitative.create(bool(args[0]))
        return Abstraction_Qualitative.create(args[0] == args[1])

    def getSubpattern(self, extension, index):
        pair_creator = ItemAbstractionPairCreator.getInstance()
        elems = []
        abstraction = None
        next_index = index + 1
        for i in range(extension.size()):
            if i != index:
                if i == next_index:
                    if abstraction is None:
                        abstraction = extension.getIthElement(i).getAbstraction()
                    elems.append(pair_creator.getItemAbstractionPair(extension.getIthElement(i).getItem(), abstraction))
                else:
                    elems.append(extension.getIthElement(i))
            else:
                if index == 0:
                    abstraction = self.CreateDefaultAbstraction()
                else:
                    removed_abs = extension.getIthElement(i).getAbstraction()
                    if not removed_abs.hasEqualRelation():
                        abstraction = self.createAbstraction(False)
        return PatternCreator.getInstance().createPattern(elems)

    def generateCandidates(self, creator, p1, p2, minSupport):
        different = False
        e1, e2 = p1.getElements(), p2.getElements()
        for i in range(len(e1) - 1):
            a = e1[i + 1]
            b = e2[i]
            if i == 0:
                if a.getItem() != b.getItem():
                    different = True
                    break
            else:
                if a != b:
                    different = True
                    break
        if different:
            return None

        inter = p1.getAppearingIn().clone()
        inter.and_op(p2.getAppearingIn())
        if inter.cardinality() >= minSupport:
            c = p1.clonePattern()
            c.add(p2.getLastElement())
            return c
        return None

    def isCandidateInSequence(self, finder, candidate, sequence, k, i, position):
        finder.isCandidatePresentInTheSequence_qualitative(candidate, sequence, k, 0, position)

    def findPositionOfItemInSequence(self, sequence, itemPar, absPar, absAnterior, indexItemset, indexitem, indexItemsetAnterior, indexitemAnterior):
        if absPar.hasEqualRelation():
            if indexItemset == indexItemsetAnterior:
                return sequence.SearchForItemAtTheSameItemset(itemPar, indexItemset, indexitem)
            return None
        itemset_to_search = indexItemset
        item_to_search = indexitem
        if indexItemset == indexItemsetAnterior:
            itemset_to_search += 1
            item_to_search = 0
        return sequence.searchForTheFirstAppearance(itemset_to_search, item_to_search, itemPar)

    def generateSize2Candidates(self, creator, pat1, pat2):
        pair_creator = ItemAbstractionPairCreator.getInstance()
        p1 = pat1.getIthElement(0)
        p2 = pat2.getIthElement(0)
        out = []
        out.append(PatternCreator.getInstance().createPattern([
            p1,
            pair_creator.getItemAbstractionPair(p2.getItem(), Abstraction_Qualitative.create(False)),
        ]))
        if p1 != p2:
            out.append(PatternCreator.getInstance().createPattern([
                p2,
                pair_creator.getItemAbstractionPair(p1.getItem(), Abstraction_Qualitative.create(False)),
            ]))
        return out

class AlgoSPM_FC_L:
    def __init__(self, minSupRelative, mingap, maxgap, windowSize, abstractionCreator):
        self.minSupRelative = minSupRelative
        self.minGap = mingap
        self.maxGap = maxgap
        self.windowSize = windowSize
        self.abstractionCreator = abstractionCreator
        self.minSupAbsolute = 0
        self.patterns = None
        self.start = 0
        self.end = 0
        self.frequentItems = []
        self.isSorted = False
        self.numberOfFrequentPatterns = 0
        self.writer = None
        self.outputSequenceIdentifiers = False

    def runAlgorithm(self, database, keepPatterns, verbose, outputFilePath, outputSequenceIdentifiers, alpha, beta, gamma):
        self.outputSequenceIdentifiers = outputSequenceIdentifiers
        self.patterns = Sequences("FREQUENT SEQUENTIAL PATTERNS")
        self.writer = None if outputFilePath is None else open(outputFilePath, "w", encoding="utf-8")

        self.minSupAbsolute = int(math.ceil(self.minSupRelative * database.size()))
        if self.minSupAbsolute == 0:
            self.minSupAbsolute = 1

        candidateGenerator = CandidateGeneration()
        supportCounter = SupportCounting(database, self.abstractionCreator, alpha, beta, gamma)

        MemoryLogger.getInstance().reset()
        self.start = int(time.time() * 1000)
        self.runGsp(database, candidateGenerator, supportCounter, keepPatterns, verbose)
        self.end = int(time.time() * 1000)

        if self.writer is not None:
            self.writer.close()
        return self.patterns

    def runGsp(self, database, candidateGenerator, supportCounter, keepPatterns, verbose):
        self.frequentItems = database.frequentItems()
        self.patterns.addSequences(self.frequentItems, 1)
        frequentSet = set(self.frequentItems)
        indexationMap = {}
        self.numberOfFrequentPatterns += len(self.frequentItems)
        k = 1

        while frequentSet:
            k += 1
            candidateSet = candidateGenerator.generateCandidates(frequentSet, self.abstractionCreator, indexationMap, k, self.minSupAbsolute)
            frequentSet = None
            if candidateSet is None:
                break

            MemoryLogger.getInstance().checkMemory()
            frequentSet = supportCounter.countSupport(candidateSet, k, self.minSupAbsolute)
            MemoryLogger.getInstance().checkMemory()

            self.numberOfFrequentPatterns += len(frequentSet)
            indexationMap = supportCounter.getIndexationMap()
            self.patterns.addSequences(list(frequentSet), k)

            level = k - 1
            if not keepPatterns:
                if frequentSet:
                    self.patterns.delete(level)
            elif self.writer is not None:
                if frequentSet:
                    for seq in self.patterns.getLevel(level):
                        self.writer.write(seq.toStringToFile(self.outputSequenceIdentifiers))
                        self.writer.write("\n")
                    self.patterns.delete(level)

        if keepPatterns and self.writer is not None:
            level = self.patterns.getLevelCount()
            for seq in self.patterns.getLevel(level):
                self.writer.write(seq.toStringToFile(self.outputSequenceIdentifiers))
                self.writer.write("\n")
            self.patterns.delete(level)

        MemoryLogger.getInstance().checkMemory()

    def printStatistics(self):
        if not self.isSorted:
            self.patterns.sort()
            self.isSorted = True
        sb = []
        sb.append("=============  SPM_FC_L v.2.58 - STATISTICS =============")
        sb.append(os.linesep)
        sb.append(" Total time ~ ")
        sb.append(str(self.runningTime()))
        sb.append(" ms")
        sb.append(os.linesep)
        sb.append(" Frequent sequences count : ")
        sb.append(str(self.numberOfFrequentPatterns))
        sb.append(os.linesep)
        sb.append(" Max memory (mb):")
        sb.append(str(MemoryLogger.getInstance().getMaxMemory()))
        sb.append(os.linesep)
        sb.append("===================================================\n")
        if self.writer is None:
            sb.append(self.patterns.toStringToFile(False))
        return "".join(sb)

    def runningTime(self):
        return self.end - self.start


class RemoveStatics:
    @staticmethod
    def clear():
        ItemAbstractionPairCreator.clear()
        Abstraction_Qualitative.clear()
        AbstractionCreator_Qualitative.sclear()
        PatternCreator.sclear()


def fileToPath(filename):
    return os.path.join(os.path.dirname(__file__), filename)


def run_save_to_file(minimum_support=0.1, alpha=(0.5 / 3.0), beta=(1.5 / 3.0), gamma=(1.0 / 3.0), mingap=0.0, maxgap=float("inf"), window_size=0.0):
    keepPatterns = True
    verbose = False
    outputSequenceIdentifiers = False

    inputFilePath = fileToPath("mooc_small.txt")
    outputFilePath = fileToPath("output_spm_fc_l.txt")

    abstractionCreator = AbstractionCreator_Qualitative.getInstance()
    sequenceDatabase = SequenceDatabase(abstractionCreator)
    sequenceDatabase.loadFile(inputFilePath, minimum_support, alpha, beta, gamma)

    algo = AlgoSPM_FC_L(minimum_support, mingap, maxgap, window_size, abstractionCreator)
    algo.runAlgorithm(sequenceDatabase, keepPatterns, verbose, outputFilePath, outputSequenceIdentifiers, alpha, beta, gamma)
    print(algo.printStatistics())


def run_save_to_memory(minimum_support=0.08, alpha=(0.5 / 3.0), beta=(1.5 / 3.0), gamma=(1.0 / 3.0), mingap=0.0, maxgap=float("inf"), window_size=0.0):
    keepPatterns = True
    verbose = False
    outputSequenceIdentifiers = False

    inputFilePath = fileToPath("mooc_small.txt")
    abstractionCreator = AbstractionCreator_Qualitative.getInstance()
    sequenceDatabase = SequenceDatabase(abstractionCreator)
    sequenceDatabase.loadFile(inputFilePath, minimum_support, alpha, beta, gamma)

    algo = AlgoSPM_FC_L(minimum_support, mingap, maxgap, window_size, abstractionCreator)
    algo.runAlgorithm(sequenceDatabase, keepPatterns, verbose, None, outputSequenceIdentifiers, alpha, beta, gamma)
    print(algo.printStatistics())


if __name__ == "__main__":
    # Threshold (minimum support) - change here.
    MINIMUM_SUPPORT = 0.08
    run_save_to_file(minimum_support=MINIMUM_SUPPORT)


