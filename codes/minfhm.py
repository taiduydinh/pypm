import os
import pickle
import time
import tracemalloc


class AlgoMinFHM:
    def __init__(self):
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.huiCount = 0
        self.candidateCount = 0
        self.mapItemToTWU = {}
        self.writer = None
        self.mapFMAP = {}
        self.debug = False
        self.ENABLE_LA_PRUNE = True
        self.listItemsetsBySize = None

    def isSubsumingAFoundItemset(self, itemset):
        if len(itemset) == 2:
            return False

        for i in range(0, len(itemset)):
            if i >= len(self.listItemsetsBySize):
                break
            itemsets = self.listItemsetsBySize[i]
            if len(itemsets) > 0:
                for itemset_in_list in itemsets:
                    if ArraysAlgos.includedIn(itemset_in_list.itemset, itemset):
                        return True
        return False

    def registerItemsetAndRemoveLarger(self, itemset, utility, support):
        if len(itemset) == 2:
            self.mapFMAP[itemset[0]][itemset[1]] = 0

        if len(itemset) >= len(self.listItemsetsBySize):
            i = len(self.listItemsetsBySize)
            while i <= len(itemset):
                self.listItemsetsBySize.append([])
                i += 1

        self.listItemsetsBySize[len(itemset)].append(Itemset(itemset, utility, support))

        for i in range(len(itemset) + 1, len(self.listItemsetsBySize)):
            itemsets = self.listItemsetsBySize[i]
            if len(itemsets) > 0:
                filtered = []
                for itemset2 in itemsets:
                    if not ArraysAlgos.includedIn(itemset, itemset2.itemset):
                        filtered.append(itemset2)
                self.listItemsetsBySize[i] = filtered

    class Pair:
        def __init__(self):
            self.item = 0
            self.utility = 0

        def __str__(self):
            return "[" + str(self.item) + "," + str(self.utility) + "]"

    def runAlgorithm(self, input_path, output_path, min_utility):
        MemoryLogger.getInstance().reset()
        self.mapFMAP = {}
        self.startTimestamp = int(time.time() * 1000)
        self.writer = open(output_path, "w", encoding="utf-8")
        self.mapItemToTWU = {}
        map_item_to_utility = {}
        self.listItemsetsBySize = []

        my_input = None
        try:
            my_input = open(input_path, "r", encoding="utf-8")
            for this_line in my_input:
                this_line = this_line.strip()
                if (
                    this_line == ""
                    or this_line[0] == "#"
                    or this_line[0] == "%"
                    or this_line[0] == "@"
                ):
                    continue

                split = this_line.split(":")
                items = split[0].split(" ")
                transaction_utility = int(split[1])
                utility_values = split[2].split(" ")

                for i in range(len(items)):
                    item = int(items[i])
                    twu = self.mapItemToTWU.get(item)
                    twu = transaction_utility if twu is None else twu + transaction_utility
                    self.mapItemToTWU[item] = twu

                    utility = int(utility_values[i])
                    total_utility_of_item = map_item_to_utility.get(item)
                    total_utility_of_item = utility if total_utility_of_item is None else utility + total_utility_of_item
                    map_item_to_utility[item] = total_utility_of_item
        except Exception:
            import traceback

            traceback.print_exc()
        finally:
            if my_input is not None:
                my_input.close()

        list_of_utility_lists = []
        map_item_to_utility_list = {}
        for item in self.mapItemToTWU.keys():
            if self.mapItemToTWU[item] >= min_utility and map_item_to_utility[item] < min_utility:
                ulist = UtilityList(item)
                map_item_to_utility_list[item] = ulist
                list_of_utility_lists.append(ulist)

        list_of_utility_lists.sort(key=lambda ul: (self.mapItemToTWU[ul.item], ul.item))

        try:
            my_input = open(input_path, "r", encoding="utf-8")
            tid = 0
            for this_line in my_input:
                this_line = this_line.strip()
                if (
                    this_line == ""
                    or this_line[0] == "#"
                    or this_line[0] == "%"
                    or this_line[0] == "@"
                ):
                    continue

                split = this_line.split(":")
                items = split[0].split(" ")
                utility_values = split[2].split(" ")

                remaining_utility = 0
                new_twu = 0
                revised_transaction = []

                for i in range(len(items)):
                    pair = AlgoMinFHM.Pair()
                    pair.item = int(items[i])
                    pair.utility = int(utility_values[i])
                    if self.mapItemToTWU[pair.item] >= min_utility and map_item_to_utility[pair.item] < min_utility:
                        revised_transaction.append(pair)
                        remaining_utility += pair.utility
                        new_twu += pair.utility

                revised_transaction.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                for i in range(len(revised_transaction)):
                    pair = revised_transaction[i]
                    remaining_utility -= pair.utility

                    utility_list_of_item = map_item_to_utility_list[pair.item]
                    element = Element(tid, pair.utility, remaining_utility)
                    utility_list_of_item.addElement(element)

                    map_fmap_item = self.mapFMAP.get(pair.item)
                    if map_fmap_item is None:
                        map_fmap_item = {}
                        self.mapFMAP[pair.item] = map_fmap_item

                    for j in range(i + 1, len(revised_transaction)):
                        pair_after = revised_transaction[j]
                        twu_sum = map_fmap_item.get(pair_after.item)
                        if twu_sum is None:
                            map_fmap_item[pair_after.item] = new_twu
                        else:
                            map_fmap_item[pair_after.item] = twu_sum + new_twu
                tid += 1
        except Exception:
            import traceback

            traceback.print_exc()
        finally:
            if my_input is not None:
                my_input.close()

        MemoryLogger.getInstance().checkMemory()

        for item, utility in map_item_to_utility.items():
            if utility >= min_utility:
                self.writeOutItemsetSize1(item, utility)

        self.minfhm([], None, list_of_utility_lists, min_utility)

        for list_itemsets in self.listItemsetsBySize:
            for itemset in list_itemsets:
                self.writeOut(itemset)

        MemoryLogger.getInstance().checkMemory()
        self.writer.close()
        self.endTimestamp = int(time.time() * 1000)

    def compareItems(self, item1, item2):
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return item1 - item2 if compare == 0 else compare

    def minfhm(self, prefix, p_ul, uls, min_utility):
        for i in range(len(uls)):
            x = uls[i]
            if x.sumIutils + x.sumRutils >= min_utility:
                new_prefix = ArraysAlgos.appendIntegerToArray(prefix, x.item)
                ex_uls = []
                for j in range(i + 1, len(uls)):
                    y = uls[j]
                    map_twuf = self.mapFMAP.get(x.item)
                    if map_twuf is not None:
                        twu_f = map_twuf.get(y.item)
                        if twu_f is None or twu_f < min_utility:
                            continue
                    self.candidateCount += 1
                    pxy = self.construct(p_ul, x, y, min_utility)
                    if pxy is not None:
                        itemset = ArraysAlgos.appendIntegerToArray(new_prefix, y.item)
                        if pxy.sumIutils >= min_utility and not self.isSubsumingAFoundItemset(itemset):
                            self.registerItemsetAndRemoveLarger(itemset, pxy.sumIutils, len(pxy.elements))
                        elif not self.isSubsumingAFoundItemset(itemset):
                            ex_uls.append(pxy)

                if len(ex_uls) > 1:
                    self.minfhm(new_prefix, x, ex_uls, min_utility)

        MemoryLogger.getInstance().checkMemory()

    def construct(self, p, px, py, min_utility):
        pxy_ul = UtilityList(py.item)
        total_utility = px.sumIutils + px.sumRutils

        for ex in px.elements:
            ey = self.findElementWithTID(py, ex.tid)
            if ey is None:
                if self.ENABLE_LA_PRUNE:
                    total_utility -= ex.iutils + ex.rutils
                    if total_utility < min_utility:
                        return None
                continue

            if p is None:
                exy = Element(ex.tid, ex.iutils + ey.iutils, ey.rutils)
                pxy_ul.addElement(exy)
            else:
                e = self.findElementWithTID(p, ex.tid)
                if e is not None:
                    exy = Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils)
                    pxy_ul.addElement(exy)
        return pxy_ul

    def findElementWithTID(self, ulist, tid):
        data = ulist.elements
        first = 0
        last = len(data) - 1
        while first <= last:
            middle = (first + last) >> 1
            if data[middle].tid < tid:
                first = middle + 1
            elif data[middle].tid > tid:
                last = middle - 1
            else:
                return data[middle]
        return None

    def writeOut(self, itemset):
        self.huiCount += 1
        buffer = []
        for i in range(len(itemset.itemset)):
            buffer.append(str(itemset.itemset[i]))
        out = " ".join(buffer) + " #UTIL: " + str(itemset.utility)
        self.writer.write(out + "\n")

    def writeOutItemsetSize1(self, item, utility):
        self.huiCount += 1
        out = str(item) + " #UTIL: " + str(utility)
        self.writer.write(out + "\n")

    def printStats(self):
        print("=============  MinFHM ALGORITHM - STATS =============")
        print(" Total time ~ " + str(self.endTimestamp - self.startTimestamp) + " ms")
        print(" Memory ~ " + str(MemoryLogger.getInstance().getMaxMemory()) + " MB")
        print(" MinHUIs count : " + str(self.huiCount))
        print(" Candidate count : " + str(self.candidateCount))

        if self.debug:
            pair_count = 0
            max_memory = self.getObjectSize(self.mapFMAP)
            for k, vmap in self.mapFMAP.items():
                max_memory += self.getObjectSize(k)
                for k2, v2 in vmap.items():
                    pair_count += 1
                    max_memory += self.getObjectSize(k2) + self.getObjectSize(v2)
            print("CMAP size " + str(max_memory) + " MB")
            print("PAIR COUNT " + str(pair_count))
        print("===================================================")

    def getObjectSize(self, obj):
        data = pickle.dumps(obj)
        return len(data) / 1024.0 / 1024.0


class ArraysAlgos:
    @staticmethod
    def cloneItemSetMinusOneItem(itemset, item_to_remove):
        new_itemset = [0] * (len(itemset) - 1)
        i = 0
        for value in itemset:
            if value != item_to_remove:
                new_itemset[i] = value
                i += 1
        return new_itemset

    @staticmethod
    def cloneItemSetMinusAnItemset(itemset, itemset_to_not_keep):
        sorted_not_keep = sorted(itemset_to_not_keep)
        result = []
        for value in itemset:
            if ArraysAlgos.binarySearch(sorted_not_keep, value) < 0:
                result.append(value)
        return result

    @staticmethod
    def allTheSameExceptLastItem(itemset1, itemset2):
        for i in range(len(itemset1) - 1):
            if itemset1[i] != itemset2[i]:
                return False
        return True

    @staticmethod
    def concatenate(prefix, suffix):
        return list(prefix) + list(suffix)

    @staticmethod
    def intersectTwoSortedArrays(array1, array2):
        pos1 = 0
        pos2 = 0
        result = []
        while pos1 < len(array1) and pos2 < len(array2):
            if array1[pos1] < array2[pos2]:
                pos1 += 1
            elif array2[pos2] < array1[pos1]:
                pos2 += 1
            else:
                result.append(array1[pos1])
                pos1 += 1
                pos2 += 1
        return result

    @staticmethod
    def containsOrEquals(itemset1, itemset2):
        i = 0
        j = 0
        while i < len(itemset1) and j < len(itemset2):
            if itemset1[i] == itemset2[j]:
                i += 1
                j += 1
            elif itemset1[i] > itemset2[j]:
                return False
            else:
                i += 1
        return j == len(itemset2)

    @staticmethod
    def containsLEX(itemset, item, max_item_in_array=None):
        if max_item_in_array is not None and item > max_item_in_array:
            return False
        for value in itemset:
            if value == item:
                return True
            if value > item:
                return False
        return False

    @staticmethod
    def sameAs(itemset1, itemset2, pos_removed):
        j = 0
        for i in range(len(itemset1)):
            if j == pos_removed:
                j += 1
            if itemset1[i] == itemset2[j]:
                j += 1
            elif itemset1[i] > itemset2[j]:
                return 1
            else:
                return -1
        return 0

    @staticmethod
    def includedIn(itemset1, itemset2):
        count = 0
        for value in itemset2:
            if count < len(itemset1) and value == itemset1[count]:
                count += 1
                if count == len(itemset1):
                    return True
        return False

    @staticmethod
    def includedIn_length(itemset1, itemset1_length, itemset2):
        count = 0
        for value in itemset2:
            if count < itemset1_length and value == itemset1[count]:
                count += 1
                if count == itemset1_length:
                    return True
        return False

    @staticmethod
    def containsLEXPlus(itemset, item):
        for value in itemset:
            if value == item:
                return True
            if value > item:
                return True
        return False

    @staticmethod
    def contains(itemset, item):
        for value in itemset:
            if value == item:
                return True
            if value > item:
                return False
        return False

    @staticmethod
    def appendIntegerToArray(array, integer):
        newgen = [0] * (len(array) + 1)
        newgen[0 : len(array)] = array
        newgen[len(array)] = integer
        return newgen

    @staticmethod
    def convertStringArrayToDoubleArray(tokens):
        return [float(token) for token in tokens]

    @staticmethod
    def isSubsetOf(itemset1, itemset2):
        if itemset1 is None or len(itemset1) == 0:
            return True
        for val in itemset1:
            found = False
            for value in itemset2:
                if value > val:
                    return False
                if val == value:
                    found = True
                    break
            if not found:
                return False
        return True

    @staticmethod
    def comparatorItemsetSameSize(itemset1, itemset2):
        for i in range(len(itemset1)):
            if itemset1[i] < itemset2[i]:
                return -1
            if itemset2[i] < itemset1[i]:
                return 1
        return 0

    @staticmethod
    def binarySearch(values, value):
        first = 0
        last = len(values) - 1
        while first <= last:
            middle = (first + last) >> 1
            if values[middle] < value:
                first = middle + 1
            elif values[middle] > value:
                last = middle - 1
            else:
                return middle
        return -1


class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class Itemset:
    def __init__(self, itemset=None, utility=0, support=0):
        self.itemset = [] if itemset is None else list(itemset)
        self.utility = utility
        self.support = support

    def __str__(self):
        return str(self.itemset) + " utility : " + str(self.utility) + " support:  " + str(self.support)


class MainTestMinFHM:
    @staticmethod
    def fileToPath(filename):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    @staticmethod
    def main(arg=None):
        del arg
        input_path = MainTestMinFHM.fileToPath("DB_Utility.txt")
        output_path = MainTestMinFHM.fileToPath("output_python.txt")
        min_utility = 25

        algorithm = AlgoMinFHM()
        algorithm.runAlgorithm(input_path, output_path, min_utility)
        algorithm.printStats()


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
        current_memory, _peak = tracemalloc.get_traced_memory()
        current_memory_mb = current_memory / 1024.0 / 1024.0
        if current_memory_mb > self.maxMemory:
            self.maxMemory = current_memory_mb
        return current_memory_mb


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
    MainTestMinFHM.main()
