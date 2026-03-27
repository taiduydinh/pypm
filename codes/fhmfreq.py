import time
import psutil
import os
import pickle
import urllib.parse
from collections import defaultdict

class Element:
    def __init__(self, tid, iutils, rutils):
        self.tid = tid
        self.iutils = iutils
        self.rutils = rutils

class UtilityList:
    def __init__(self, item):
        self.item = item
        self.sum_iutils = 0
        self.sum_rutils = 0
        self.elements = []

    def add_element(self, element):
        self.sum_iutils += element.iutils
        self.sum_rutils += element.rutils
        self.elements.append(element)

    def get_support(self):
        return len(self.elements)

class MemoryLogger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MemoryLogger, cls).__new__(cls)
            cls._instance.max_memory = 0
        return cls._instance

    def get_max_memory(self):
        return self._instance.max_memory

    def reset(self):
        self._instance.max_memory = 0

    def check_memory(self):
        process = psutil.Process(os.getpid())
        current_memory = process.memory_info().rss / 1024 / 1024
        if current_memory > self._instance.max_memory:
            self._instance.max_memory = current_memory
        return current_memory

class AlgoFHM_Freq:
    def __init__(self):
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.hui_count = 0
        self.candidate_count = 0
        self.map_item_to_TWU = {}
        self.map_item_to_support = {}
        self.writer = None
        self.map_FMAP = defaultdict(dict)
        self.ENABLE_LA_PRUNE = True
        self.DEBUG = False
        self.BUFFERS_SIZE = 200
        self.itemset_buffer = [0] * self.BUFFERS_SIZE
        self.minsup_relative = 0

    class Pair:
        def __init__(self, item=0, utility=0):
            self.item = item
            self.utility = utility

    def run_algorithm(self, input, output, min_utility, minsupp):
        MemoryLogger().reset()
        self.itemset_buffer = [0] * self.BUFFERS_SIZE
        self.map_FMAP = defaultdict(dict)
        self.start_timestamp = time.time()
        self.writer = open(output, 'w')
        self.map_item_to_TWU = {}
        self.map_item_to_support = {}
        database_size = 0
        try:
            with open(input, 'r') as file:
                for line in file:
                    if line.strip() == '' or line[0] in ['#', '%', '@']:
                        continue
                    split = line.split(":")
                    items = split[0].split(" ")
                    transaction_utility = int(split[1])
                    database_size += 1
                    for item in items:
                        item = int(item)
                        self.map_item_to_TWU[item] = self.map_item_to_TWU.get(item, 0) + transaction_utility
                        self.map_item_to_support[item] = self.map_item_to_support.get(item, 0) + 1
        except Exception as e:
            print(e)

        self.minsup_relative = int(minsupp * database_size)

        list_of_utility_lists = []
        map_item_to_utility_list = {}

        for item, twu in self.map_item_to_TWU.items():
            if twu >= min_utility and self.map_item_to_support[item] >= self.minsup_relative:
                u_list = UtilityList(item)
                map_item_to_utility_list[item] = u_list
                list_of_utility_lists.append(u_list)

        list_of_utility_lists.sort(key=lambda x: self.compare_items(x.item, x.item))

        try:
            with open(input, 'r') as file:
                tid = 0
                for line in file:
                    if line.strip() == '' or line[0] in ['#', '%', '@']:
                        continue
                    split = line.split(":")
                    items = split[0].split(" ")
                    utility_values = split[2].split(" ")
                    remaining_utility = 0
                    new_TWU = 0
                    revised_transaction = []
                    for item, utility in zip(items, utility_values):
                        pair = self.Pair(int(item), int(utility))
                        if self.map_item_to_TWU[pair.item] >= min_utility and self.map_item_to_support[pair.item] >= self.minsup_relative:
                            revised_transaction.append(pair)
                            remaining_utility += pair.utility
                            new_TWU += pair.utility
                    revised_transaction.sort(key=lambda x: self.compare_items(x.item, x.item))
                    for i, pair in enumerate(revised_transaction):
                        remaining_utility -= pair.utility
                        utility_list_of_item = map_item_to_utility_list[pair.item]
                        element = Element(tid, pair.utility, remaining_utility)
                        utility_list_of_item.add_element(element)
                        map_FMAP_item = self.map_FMAP[pair.item]
                        for pair_after in revised_transaction[i+1:]:
                            map_FMAP_item[pair_after.item] = map_FMAP_item.get(pair_after.item, 0) + new_TWU
                    tid += 1
        except Exception as e:
            print(e)

        MemoryLogger().check_memory()
        self.fhm(self.itemset_buffer, 0, None, list_of_utility_lists, min_utility)
        MemoryLogger().check_memory()
        self.writer.close()
        self.end_timestamp = time.time()

    def compare_items(self, item1, item2):
        compare = self.map_item_to_TWU.get(item1, 0) - self.map_item_to_TWU.get(item2, 0)
        return item1 - item2 if compare == 0 else compare

    def fhm(self, prefix, prefix_length, pUL, ULs, min_utility):
        for i in range(len(ULs)):
            X = ULs[i]
            if X.sum_iutils >= min_utility:
                self.write_out(prefix, prefix_length, X.item, X.sum_iutils, X.get_support())
            if X.sum_iutils + X.sum_rutils >= min_utility:
                ex_ULs = []
                for j in range(i + 1, len(ULs)):
                    Y = ULs[j]
                    map_TWUF = self.map_FMAP.get(X.item)
                    if map_TWUF:
                        twuF = map_TWUF.get(Y.item)
                        if not twuF or twuF < min_utility:
                            continue
                    self.candidate_count += 1
                    temp = self.construct(pUL, X, Y, min_utility)
                    if temp:
                        ex_ULs.append(temp)
                self.itemset_buffer[prefix_length] = X.item
                self.fhm(self.itemset_buffer, prefix_length + 1, X, ex_ULs, min_utility)
        MemoryLogger().check_memory()

    def construct(self, P, px, py, min_utility):
        pxy_UL = UtilityList(py.item)
        total_utility = px.sum_iutils + px.sum_rutils
        total_support = px.get_support()
        for ex in px.elements:
            ey = self.find_element_with_tid(py, ex.tid)
            if not ey:
                if self.ENABLE_LA_PRUNE:
                    total_utility -= (ex.iutils + ex.rutils)
                    if total_utility < min_utility:
                        return None
                    total_support -= 1
                    if total_support < self.minsup_relative:
                        return None
                continue
            if not P:
                eXY = Element(ex.tid, ex.iutils + ey.iutils, ey.rutils)
                pxy_UL.add_element(eXY)
            else:
                e = self.find_element_with_tid(P, ex.tid)
                if e:
                    eXY = Element(ex.tid, ex.iutils + ey.iutils - e.iutils, ey.rutils)
                    pxy_UL.add_element(eXY)
        return pxy_UL

    def find_element_with_tid(self, ulist, tid):
        list_elements = ulist.elements
        first, last = 0, len(list_elements) - 1
        while first <= last:
            middle = (first + last) // 2
            if list_elements[middle].tid < tid:
                first = middle + 1
            elif list_elements[middle].tid > tid:
                last = middle - 1
            else:
                return list_elements[middle]
        return None

    def write_out(self, prefix, prefix_length, item, utility, support):
        self.hui_count += 1
        buffer = []
        for i in range(prefix_length):
            buffer.append(str(prefix[i]))
            buffer.append(' ')
        buffer.append(str(item))
        buffer.append(" #UTIL: ")
        buffer.append(str(utility))
        buffer.append(" #SUP: ")
        buffer.append(str(support))
        line = ''.join(buffer)
        self.writer.write(line + '\n')
        print(line)
        print()

    def print_stats(self):
        print("=============  FHM-Freq ALGORITHM - STATS =============")
        print(f" Total time ~ {self.end_timestamp - self.start_timestamp} ms")
        print(f" Memory ~ {MemoryLogger().get_max_memory()} MB")
        print(f" High-utility itemsets count : {self.hui_count}")
        print(f" Candidate count : {self.candidate_count}")
        if self.DEBUG:
            pair_count = 0
            max_memory = self.get_object_size(self.map_FMAP)
            for key, value in self.map_FMAP.items():
                max_memory += self.get_object_size(key)
                for k, v in value.items():
                    pair_count += 1
                    max_memory += self.get_object_size(k) + self.get_object_size(v)
            print(f"CMAP size {max_memory} MB")
            print(f"PAIR COUNT {pair_count}")
        print("===================================================")

    def get_object_size(self, obj):
        return len(pickle.dumps(obj)) / 1024 / 1024

def file_to_path(filename):
    return urllib.parse.unquote(os.path.join(os.path.dirname(__file__), filename))

if __name__ == "__main__":
    input_file = file_to_path("DB_Utility.txt")
    output_file = "output.txt"

    min_utility = 40
    minsup = 0.1 # which means 10% of the database size

    # Applying the HUIMiner algorithm
    fhmfreq = AlgoFHM_Freq()
    fhmfreq.run_algorithm(input_file, output_file, min_utility, minsup)
    fhmfreq.print_stats()
