import os
import time
import tracemalloc


class AlgoHUGMiner:
    def __init__(self):
        self.maxMemory = 0.0
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.hugsCount = 0
        self.candidateCount = 0
        self.candidateAvoidedbyFHMPruning = 0
        self.generatorChecks = 0
        self.partiallyAvoidedOrAvoidedGeneratorChecks = 0
        self.mapItemToTWU = {}
        self.writer = None
        self.mapFMAP = {}
        self.transactionCount = 0
        self.debug = False
        self.mapItemToUtilityList = {}
        self.itemsetBuffer = None
        self.BUFFERS_SIZE = 200
        self.enableLAPrune = True
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    def runAlgorithm(self, input_path, output_path, min_utility):
        self.maxMemory = 0.0
        self.transactionCount = 0
        self.itemsetBuffer = [0] * self.BUFFERS_SIZE
        self.mapFMAP = {}
        self.startTimestamp = int(time.time() * 1000)
        self.writer = open(output_path, "w", encoding="utf-8")
        self.mapItemToTWU = {}

        total_utility = 0
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
                for token in items:
                    item = int(token)
                    twu = self.mapItemToTWU.get(item)
                    twu = transaction_utility if twu is None else twu + transaction_utility
                    self.mapItemToTWU[item] = twu
                self.transactionCount += 1
                total_utility += transaction_utility
        except Exception:
            import traceback

            traceback.print_exc()
        finally:
            if my_input is not None:
                my_input.close()

        list_of_utility_lists = []
        self.mapItemToUtilityList = {}
        for item in self.mapItemToTWU.keys():
            if self.mapItemToTWU[item] >= min_utility:
                ulist = UtilityListWithCriticalObjects(item)
                self.mapItemToUtilityList[item] = ulist
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

                new_tu = 0
                revised_transaction = []
                for i in range(len(items)):
                    pair = PairItemUtility()
                    pair.item = int(items[i])
                    pair.utility = int(utility_values[i])
                    if self.mapItemToTWU.get(pair.item, 0) >= min_utility:
                        revised_transaction.append(pair)
                        new_tu += pair.utility

                revised_transaction.sort(key=lambda p: (self.mapItemToTWU[p.item], p.item))

                remaining_utility = new_tu
                for i in range(len(revised_transaction)):
                    pair = revised_transaction[i]
                    remaining_utility -= pair.utility

                    utility_list_of_item = self.mapItemToUtilityList[pair.item]
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
                            map_fmap_item[pair_after.item] = new_tu
                        else:
                            map_fmap_item[pair_after.item] = twu_sum + new_tu
                tid += 1
        except Exception:
            import traceback

            traceback.print_exc()
        finally:
            if my_input is not None:
                my_input.close()

        self.checkMemory()

        tidset_empty_set = set(range(1, self.transactionCount))
        empty_ul = UtilityListWithCriticalObjects(None)
        empty_ul.tidset = tidset_empty_set
        empty_ul.crit = []
        empty_set = []

        filtered_uls = []
        for ul in list_of_utility_lists:
            self.checkIfGeneratorSingleItem(empty_ul, ul)
            keep = True
            if ul.crit is None or (ul.sumIutils + ul.sumRutils < min_utility):
                keep = False
            else:
                if ul.sumIutils >= min_utility:
                    # Kept as Java implementation: support value uses transactionCount.
                    self.writeOut(empty_set, 0, ul.item, ul.sumIutils, self.transactionCount)
            if keep:
                filtered_uls.append(ul)
        list_of_utility_lists = filtered_uls

        if total_utility >= min_utility:
            self.hugminer(self.itemsetBuffer, 0, empty_ul, list_of_utility_lists, min_utility)

        self.checkMemory()
        self.writer.close()
        self.endTimestamp = int(time.time() * 1000)

    def compareItems(self, item1, item2):
        compare = self.mapItemToTWU[item1] - self.mapItemToTWU[item2]
        return item1 - item2 if compare == 0 else compare

    def hugminer(self, prefix_p, prefix_length, p_ul, extensions_uls, min_utility):
        for i in range(len(extensions_uls)):
            p_x_ul = extensions_uls[i]
            if p_x_ul.sumIutils + p_x_ul.sumRutils >= min_utility:
                self.itemsetBuffer[prefix_length] = p_x_ul.item
                extensions_of_px = []

                for j in range(i + 1, len(extensions_uls)):
                    p_y_ul = extensions_uls[j]
                    should_prune = self.checkEUCPStrategy(min_utility, p_x_ul.item, p_y_ul.item)
                    if should_prune:
                        continue
                    self.candidateCount += 1

                    if p_ul.item is None:
                        p_xy_ul = self.construct_two_items(p_x_ul, p_y_ul, min_utility)
                    else:
                        p_xy_ul = self.construct_general(p_ul, p_x_ul, p_y_ul, min_utility)

                    if p_xy_ul is None or len(p_xy_ul.elements) == 0:
                        continue

                    if (
                        len(p_xy_ul.elements) == len(p_x_ul.elements)
                        or len(p_xy_ul.elements) == len(p_y_ul.elements)
                    ):
                        continue

                    if p_xy_ul.sumIutils + p_xy_ul.sumRutils < min_utility:
                        continue

                    is_generator = self.checkIfGenerator(p_x_ul, p_xy_ul, prefix_length + 1)
                    if is_generator:
                        if p_xy_ul.sumIutils >= min_utility:
                            self.writeOut(
                                self.itemsetBuffer,
                                prefix_length + 1,
                                p_xy_ul.item,
                                p_xy_ul.sumIutils,
                                len(p_xy_ul.elements),
                            )
                        extensions_of_px.append(p_xy_ul)

                if len(extensions_of_px) > 1:
                    self.hugminer(self.itemsetBuffer, prefix_length + 1, p_x_ul, extensions_of_px, min_utility)

    def checkEUCPStrategy(self, min_utility, item_x, item_y):
        map_twuf = self.mapFMAP.get(item_x)
        if map_twuf is not None:
            twu_f = map_twuf.get(item_y)
            if twu_f is None or twu_f < min_utility:
                self.candidateAvoidedbyFHMPruning += 1
                return True
        return False

    def contains(self, values, integer):
        for item in values:
            if item == integer:
                return True
        return False

    def checkIfGenerator(self, p_ul, p_x_ul, prefix_size):
        self.generatorChecks += 1
        tidset_e = self.mapItemToUtilityList[p_x_ul.item].tidset
        p_x_ul.crit = [None] * (prefix_size + 1)

        crit_e = set(p_ul.tidset)
        crit_e.difference_update(tidset_e)
        p_x_ul.crit[len(p_x_ul.crit) - 1] = crit_e

        if len(crit_e) == 0:
            self.partiallyAvoidedOrAvoidedGeneratorChecks += 1
            return False

        for j in range(prefix_size):
            p_x_ul.crit[j] = set(p_ul.crit[j])
            p_x_ul.crit[j].intersection_update(tidset_e)
            if len(p_x_ul.crit[j]) == 0:
                if j < prefix_size - 1:
                    self.partiallyAvoidedOrAvoidedGeneratorChecks += 1
                return False
        return True

    def checkIfGeneratorSingleItem(self, empty_set_ul, x_ul):
        if self.transactionCount == len(x_ul.elements):
            self.partiallyAvoidedOrAvoidedGeneratorChecks += 1
            return
        self.generatorChecks += 1
        tidset_e = self.mapItemToUtilityList[x_ul.item].tidset
        crit = set(empty_set_ul.tidset)
        crit.difference_update(tidset_e)
        x_ul.crit = [crit]

    def construct_general(self, p, px, py, min_utility):
        pxy_ul = UtilityListWithCriticalObjects(py.item)
        total_utility = px.sumIutils + px.sumRutils

        for ex in px.elements:
            ey = self.findElementWithTID(py, ex.tid)
            if ey is None:
                if self.enableLAPrune:
                    total_utility -= ex.iutils + ex.rutils
                    if total_utility < min_utility:
                        return None
                continue

            e = self.findElementWithTID(p, ex.tid)
            if e is not None:
                diff = ey.iutils - e.iutils
                exy = Element(ex.tid, ex.iutils + diff, ey.rutils)
                pxy_ul.addElement(exy)
        return pxy_ul

    def construct_two_items(self, x_ul, y_ul, min_utility):
        pxy_ul = UtilityListWithCriticalObjects(y_ul.item)
        total_utility = x_ul.sumIutils + x_ul.sumRutils

        for ex in x_ul.elements:
            ey = self.findElementWithTID(y_ul, ex.tid)
            if ey is not None:
                exy = Element(ex.tid, ex.iutils + ey.iutils, ey.rutils)
                pxy_ul.addElement(exy)
            else:
                if self.enableLAPrune:
                    total_utility -= ex.iutils + ex.rutils
                    if total_utility < min_utility:
                        return None
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

    def writeOut(self, prefix, prefix_length, item, sum_iutils, support):
        self.hugsCount += 1
        buffer = ""
        for i in range(prefix_length):
            buffer += str(prefix[i]) + " "
        buffer += str(item)
        buffer += " #SUP: " + str(support)
        buffer += " #UTIL: " + str(sum_iutils)
        self.writer.write(buffer + "\n")

    def checkMemory(self):
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current_memory, _peak = tracemalloc.get_traced_memory()
        current_memory_mb = current_memory / 1024.0 / 1024.0
        if current_memory_mb > self.maxMemory:
            self.maxMemory = current_memory_mb

    def printStats(self):
        print("=============  HUG-Miner ALGORITHM - SPMF 0.97e - STATS =============")
        print(
            "   Candidate count : "
            + str(self.candidateCount)
            + "     (avoided by EUCP strategy : "
            + str(self.candidateAvoidedbyFHMPruning)
            + ")\n"
            + "   Genenerator checks: "
            + str(self.generatorChecks)
            + "   (partially avoided : "
            + str(self.partiallyAvoidedOrAvoidedGeneratorChecks)
            + ")"
        )
        print(" Total time ~ " + str(self.endTimestamp - self.startTimestamp) + " ms")
        print(" Max. Memory ~ " + str(self.maxMemory) + " MB")
        print(" HUGs count : " + str(self.hugsCount))
        print("==============================================================")


class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils


class MainTest_HUGMINER_saveToFile:
    @staticmethod
    def fileToPath(filename):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    @staticmethod
    def main(arg=None):
        del arg
        input_path = MainTest_HUGMINER_saveToFile.fileToPath("DB_Utility.txt")
        min_utility = 15
        output_path = MainTest_HUGMINER_saveToFile.fileToPath("output_python.txt")

        hug_miner = AlgoHUGMiner()
        hug_miner.runAlgorithm(input_path, output_path, min_utility)
        hug_miner.printStats()


class PairItemUtility:
    def __init__(self):
        self.item = 0
        self.utility = 0

    def __str__(self):
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


class UtilityListWithCriticalObjects(UtilityList):
    def __init__(self, item):
        super().__init__(item)
        self.tidset = set()
        self.crit = None

    def addElement(self, element):
        self.sumIutils += element.iutils
        self.sumRutils += element.rutils
        self.elements.append(element)
        self.tidset.add(element.tid)


if __name__ == "__main__":
    MainTest_HUGMINER_saveToFile.main()
