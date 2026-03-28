import os
import time
import tracemalloc
from typing import Dict, List, Optional, Set


class AlgoCHUD_Phase1:
    def __init__(self) -> None:
        self.tidCount = 0
        self.maxItem = 0
        self.itemCount = 0
        self.database: Dict[int, Set[int]] = {}
        self.writer = None
        self.minUtility = 0
        self.closedCount = 0
        self.totaltime = 0
        self.maxMemory = 0.0
        self.useStrategy1 = True
        self.useStrategy2 = True
        self.useStrategy3 = True
        self.useStrategy4 = True
        self.filePathInput1 = ""

    def run_algorithm(
        self,
        min_utility: int,
        file_path_input1: str,
        file_path_input2: str,
        file_path_input3: str,
        output1: str,
    ) -> None:
        self.filePathInput1 = file_path_input1
        self.minUtility = min_utility
        self.totaltime = int(time.time() * 1000)
        self.maxMemory = 0.0
        self.database = {}

        with open(file_path_input3, "r", encoding="utf-8") as reader_file3:
            line3 = reader_file3.readline().strip()
            self.tidCount = int(line3)
            line3 = reader_file3.readline().strip()
            self.maxItem = int(line3)
            line3 = reader_file3.readline().strip()
            self.itemCount = int(line3)

        table_tu = [0] * self.tidCount
        with open(file_path_input2, "r", encoding="utf-8") as reader_file2:
            for line in reader_file2:
                line = line.strip()
                if not line:
                    continue
                line_splited = line.split(":")
                table_tu[int(line_splited[0])] = int(line_splited[1])

        all_items: Set[int] = set()
        promising_items: Set[int] = set()
        table_max = [0] * (self.maxItem + 1)
        table_min = [0] * (self.maxItem + 1)

        with open(file_path_input1, "r", encoding="utf-8") as reader_file1:
            for line2 in reader_file1:
                line2 = line2.strip()
                if not line2:
                    continue
                line_splited = line2.split(":")
                item = int(line_splited[0])
                all_items.add(item)

                tids_list = line_splited[1].split(" ")
                item_tids_utilities = line_splited[2].split(" ")
                item_exact_utility = 0
                max_utility = 0
                min_utility_item = float("inf")
                for utility_string in item_tids_utilities:
                    utility = int(utility_string)
                    item_exact_utility += utility
                    if utility > max_utility:
                        max_utility = utility
                    if utility < min_utility_item:
                        min_utility_item = utility
                table_max[item] = max_utility
                table_min[item] = int(min_utility_item if min_utility_item != float("inf") else 0)

                item_estimated_utility = 0
                for tid_string in tids_list:
                    tid = int(tid_string)
                    item_estimated_utility += table_tu[tid]

                if (not self.useStrategy1) or item_estimated_utility >= self.minUtility:
                    tidset = {int(tid) for tid in tids_list}
                    self.database[item] = tidset
                    promising_items.add(item)

        self.recalculate_tu(all_items, promising_items, file_path_input1, table_tu)

        self.writer = open(output1, "w", encoding="utf-8")
        closedset: List[int] = []
        closedset_tids: Set[int] = set()
        preset: List[int] = []
        postset = list(promising_items)

        postset.sort(
            key=lambda item: (len(self.database[item]), item),
        )

        self.chud_phase1(
            True,
            closedset,
            closedset_tids,
            postset,
            preset,
            table_tu,
            table_min,
            table_max,
            0,
        )

        self.writer.close()
        self.totaltime = int(time.time() * 1000) - self.totaltime

    def print_statistics(self) -> None:
        print("========== PHASE 1 - STATS ============")
        print(" Number of transactions: " + str(self.tidCount))
        print(" Number of frequent closed itemsets: " + str(self.closedCount))
        print(" Total time ~: " + str(self.totaltime) + " ms")

    def recalculate_tu(
        self,
        items_k1: Set[int],
        items_k2: Set[int],
        file_path_input1: str,
        table_tu: List[int],
    ) -> None:
        with open(file_path_input1, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                line_splited = line.split(":")
                item = int(line_splited[0])
                if item in items_k1 and item not in items_k2:
                    tids_list = line_splited[1].split(" ")
                    item_tids_utilities = line_splited[2].split(" ")
                    for i in range(len(tids_list)):
                        tid = int(tids_list[i])
                        table_tu[tid] = table_tu[tid] - int(item_tids_utilities[i])

    def chud_phase1(
        self,
        first_time: bool,
        closedset: List[int],
        closedset_tids: Set[int],
        postset: List[int],
        preset: List[int],
        table_tu: List[int],
        table_min: List[int],
        table_max: List[int],
        level: int,
    ) -> None:
        for i in postset:
            if first_time:
                newgen_tids = self.database[i]
            else:
                newgen_tids = self.intersect_tidset(closedset_tids, self.database[i])

            twu = 0
            for tid in newgen_tids:
                twu += table_tu[tid]

            if twu >= self.minUtility:
                newgen = list(closedset)
                newgen.append(i)

                if not self.is_dup(newgen_tids, preset):
                    closedset_new = list(newgen)
                    if first_time:
                        closedset_new_tids = set(self.database[i])
                    else:
                        closedset_new_tids = set(newgen_tids)

                    postset_new: List[int] = []
                    for j in postset:
                        if self.smaller_according_to_total_order(i, j):
                            if self.database[j].issuperset(newgen_tids):
                                closedset_new.append(j)
                                j_tids = self.database[j]
                                closedset_new_tids = {tid for tid in closedset_new_tids if tid in j_tids}
                            else:
                                postset_new.append(j)

                    preset_new = list(preset)
                    table_tu_new = list(table_tu)

                    self.chud_phase1(
                        False,
                        closedset_new,
                        closedset_new_tids,
                        postset_new,
                        preset_new,
                        table_tu_new,
                        table_min,
                        table_max,
                        level + 1,
                    )

                    twu = 0
                    for tid in newgen_tids:
                        twu += table_tu[tid]

                    if twu >= self.minUtility:
                        max_utility = 0
                        for item in closedset_new:
                            max_utility += table_max[item]
                        if (not self.useStrategy4) or ((max_utility * len(closedset_new_tids)) >= self.minUtility):
                            self.write_out(closedset_new, closedset_new_tids)

                    preset.append(i)

            if first_time and self.useStrategy3:
                with open(self.filePathInput1, "r", encoding="utf-8") as reader_file1:
                    for line2 in reader_file1:
                        line2 = line2.strip()
                        if not line2:
                            continue
                        line_splited = line2.split(":")
                        item = int(line_splited[0])
                        if item == i:
                            tids = line_splited[1].split(" ")
                            utilities = line_splited[2].split(" ")
                            for k in range(len(tids)):
                                tid_int = int(tids[k])
                                utility = int(utilities[k])
                                table_tu[tid_int] = table_tu[tid_int] - utility
                            break
            elif self.useStrategy4:
                for tid in newgen_tids:
                    table_tu[tid] = table_tu[tid] - table_min[i]

    def smaller_according_to_total_order(self, i: int, j: int) -> bool:
        size1 = len(self.database[i])
        size2 = len(self.database[j])
        if size1 == size2:
            return i < j
        return (size2 - size1) > 0

    def write_out(self, closedset: List[int], tids: Set[int]) -> None:
        self.closedCount += 1
        items_part = " ".join(str(item) for item in closedset)
        tids_part = " ".join(str(tid) for tid in tids)
        self.writer.write(items_part + ":" + tids_part + "\n")

    def is_dup(self, newgen_tids: Set[int], preset: List[int]) -> bool:
        for j in preset:
            if self.database[j].issuperset(newgen_tids):
                return True
        return False

    def intersect_tidset(self, tidset1: Set[int], tidset2: Set[int]) -> Set[int]:
        if len(tidset1) > len(tidset2):
            return {tid for tid in tidset2 if tid in tidset1}
        return {tid for tid in tidset1 if tid in tidset2}

    def check_memory(self) -> None:
        current_memory = MemoryLogger.getInstance().checkMemory()
        if current_memory > self.maxMemory:
            self.maxMemory = current_memory


class AlgoCHUD_Phase2:
    def __init__(self) -> None:
        self.startTimestamp = 0
        self.totaltime = 0
        self.huiCount = 0
        self.maxMemory = 0.0
        self.maximumNumberOfTransactions = 2**31 - 1

    def run_algorithm(
        self,
        path: str,
        file_path_phase1: str,
        file_path_output: str,
        min_utility: int,
        phase2_save_huis_in_one_file: bool,
    ) -> None:
        del phase2_save_huis_in_one_file
        self.startTimestamp = int(time.time() * 1000)
        MemoryLogger.getInstance().reset()

        db = UtilityTransactionDatabaseTP()
        db.load_file(path)

        with open(file_path_phase1, "r", encoding="utf-8") as reader, open(
            file_path_output, "w", encoding="utf-8"
        ) as writer:
            line = reader.readline()
            if line == "":
                line = None
            tidcount = 0

            while line is not None and len(line) > 2:
                line_splited = line.strip().split(":")
                itemset_str = line_splited[0].split(" ")
                itemset = [int(item) for item in itemset_str]
                tidset_str = line_splited[1].split(" ")
                utility = 0

                itemset.sort()

                for tid_str in tidset_str:
                    tid = int(tid_str)
                    transaction = db.getTransactions()[tid]
                    pos = 0
                    for item in itemset:
                        while item != transaction.getItems()[pos]:
                            pos += 1
                        utility += transaction.getItemsUtilities()[pos]
                        pos += 1

                line = reader.readline()
                if line == "":
                    line = None

                if utility >= min_utility:
                    self.huiCount += 1
                    items_str = " ".join(str(item) for item in itemset)
                    writer.write(f"{items_str} #UTIL: {utility}")
                    if line is not None:
                        writer.write("\n")

                MemoryLogger.getInstance().checkMemory()
                tidcount += 1
                if tidcount == self.maximumNumberOfTransactions:
                    break

        self.totaltime = int(time.time() * 1000) - self.startTimestamp
        self.maxMemory = MemoryLogger.getInstance().getMaxMemory()

    def set_max_number_of_transactions(self, maximum_number_of_transactions: int) -> None:
        self.maximumNumberOfTransactions = maximum_number_of_transactions


class AlgoCHUD:
    def __init__(self) -> None:
        self.maximumNumberOfTransactions = 2**31 - 1
        self.totalTime = 0.0
        self.patternCount = 0.0
        self.totalMemory = 0.0
        self.DEBUG = False

    def run_algorithm(self, dataset: str, output: str, min_utility: int) -> None:
        vertical = dataset + "_vertical.txt"
        vertical2 = dataset + "_vertical2.txt"
        vertical3 = dataset + "_vertical3.txt"

        if not os.path.exists(vertical):
            converter = AlgoConvertToVerticalDatabase()
            converter.set_max_number_of_transactions(self.maximumNumberOfTransactions)
            converter.run(dataset, vertical, vertical2, vertical3)
            if self.DEBUG:
                print("FINISHED CONVERTING DATABASE TO VERTICAL FORMAT")
                print(
                    "Time conversion: "
                    + str(converter.totaltime / 1000)
                    + "s   ("
                    + str(converter.totaltime)
                    + " ms)"
                )

        output_phase1 = output + "_phase1.txt"
        if os.path.exists(output_phase1):
            os.remove(output_phase1)

        start_time = int(time.time() * 1000)

        if self.DEBUG:
            print("PHASE 1 of CHUD")
        phase1 = AlgoCHUD_Phase1()
        phase1.run_algorithm(min_utility, vertical, vertical2, vertical3, output_phase1)
        if self.DEBUG:
            print("Number of transactions : " + str(self.maximumNumberOfTransactions))
            print("Time phase1: " + str(phase1.totaltime / 1000) + "s   (" + str(phase1.totaltime) + " ms)")
            print("Closed candidates : " + str(phase1.closedCount))
            print("Max memory : " + str(phase1.maxMemory))
            print("-------------------------")

        if self.DEBUG:
            print("PHASE 2 of CHUD")
        for i in range(1, 100):
            out2 = f"L{i}.txt"
            if not os.path.exists(out2):
                break
            os.remove(out2)

        phase2 = AlgoCHUD_Phase2()
        phase2.set_max_number_of_transactions(self.maximumNumberOfTransactions)
        phase2.run_algorithm(dataset, output_phase1, output, min_utility, True)

        if self.DEBUG:
            print("Time phase2: " + str(phase2.totaltime / 1000) + "s   (" + str(phase2.totaltime) + " ms)")
            print("Closed HUI: " + str(phase2.huiCount))
            print("Max memory : " + str(phase2.maxMemory))
            print("-------------------------")
            print("=========== CHUD RESULTS========")

        self.totalMemory = phase1.maxMemory if phase1.maxMemory > phase2.maxMemory else phase2.maxMemory
        self.totalTime = int(time.time() * 1000) - start_time
        self.patternCount = phase2.huiCount

        for tmp_file in [vertical, vertical2, vertical3, output_phase1]:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

    def set_max_number_of_transactions(self, maximum_number_of_transactions: int) -> None:
        self.maximumNumberOfTransactions = maximum_number_of_transactions

    def print_stats(self) -> None:
        print("=============  CHUD v.2.26 - STATS =============")
        print("Total execution time : " + str(self.totalTime))
        print("Max memory usage: " + str(self.totalMemory) + " MB")
        print("Closed high utility itemset count: " + str(self.patternCount))
        print("===================================================")


class AlgoConvertToVerticalDatabase:
    class ItemStructure:
        def __init__(self) -> None:
            self.item = 0
            self.tidset: List[int] = []
            self.utilitiesForEachTid: List[int] = []

        def __eq__(self, obj: object) -> bool:
            if obj is self:
                return True
            if not isinstance(obj, AlgoConvertToVerticalDatabase.ItemStructure):
                return False
            return obj.item == self.item

        def __hash__(self) -> int:
            return hash(str(self.item))

    def __init__(self) -> None:
        self.totaltime = 0
        self.maximumNumberOfTransactions = 2**31 - 1

    def run(self, input_path: str, vertical: str, vertical2: str, vertical3: str) -> None:
        self.totaltime = int(time.time() * 1000)
        max_item = -1

        map_structures: Dict[int, AlgoConvertToVerticalDatabase.ItemStructure] = {}
        map_tid_tu: Dict[int, int] = {}

        with open(input_path, "r", encoding="utf-8") as reader:
            tidcount = 0
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                line_splited = line.split(":")
                transaction_utility = int(line_splited[1])
                map_tid_tu[tidcount] = transaction_utility

                transaction_items = line_splited[0].split(" ")
                transaction_items_utility = line_splited[2].split(" ")

                for i in range(len(transaction_items)):
                    item_value = int(transaction_items[i])
                    structure = map_structures.get(item_value)
                    if structure is None:
                        structure = AlgoConvertToVerticalDatabase.ItemStructure()
                        structure.item = item_value
                        if item_value > max_item:
                            max_item = item_value
                        map_structures[item_value] = structure
                    structure.tidset.append(tidcount)
                    structure.utilitiesForEachTid.append(int(transaction_items_utility[i]))

                tidcount += 1
                if tidcount == self.maximumNumberOfTransactions:
                    break

        list_items = sorted(map_structures.keys())

        with open(vertical, "w", encoding="utf-8") as writer:
            for item in list_items:
                structure = map_structures[item]
                tids_str = " ".join(str(tid) for tid in structure.tidset)
                util_str = " ".join(str(u) for u in structure.utilitiesForEachTid)
                writer.write(f"{structure.item}:{tids_str}:{util_str}\n")

        with open(vertical2, "w", encoding="utf-8") as writer2:
            entries = list(map_tid_tu.items())
            for idx, (tid, tu) in enumerate(entries):
                writer2.write(f"{tid}:{tu}")
                if idx != len(entries) - 1:
                    writer2.write("\n")

        with open(vertical3, "w", encoding="utf-8") as writer3:
            writer3.write(f"{tidcount}\n")
            writer3.write(f"{max_item}\n")
            writer3.write(f"{len(map_structures.keys())}\n")

        self.totaltime = int(time.time() * 1000) - self.totaltime

    def set_max_number_of_transactions(self, maximum_number_of_transactions: int) -> None:
        self.maximumNumberOfTransactions = maximum_number_of_transactions


class MainTestCHUD:
    @staticmethod
    def file_to_path(filename: str) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    @staticmethod
    def main() -> None:
        input_file_path = MainTestCHUD.file_to_path("DB_Utility.txt")
        output_file_path = MainTestCHUD.file_to_path("output_python.txt")
        min_utility = 20

        algo = AlgoCHUD()
        algo.run_algorithm(input_file_path, output_file_path, min_utility)
        algo.print_stats()


class MemoryLogger:
    _instance: Optional["MemoryLogger"] = None

    def __init__(self) -> None:
        self.maxMemory = 0.0
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def getMaxMemory(self) -> float:
        return self.maxMemory

    def reset(self) -> None:
        self.maxMemory = 0.0

    def checkMemory(self) -> float:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current_memory, _peak = tracemalloc.get_traced_memory()
        current_memory_mb = current_memory / 1024.0 / 1024.0
        if current_memory_mb > self.maxMemory:
            self.maxMemory = current_memory_mb
        return current_memory_mb


class TransactionTP:
    def __init__(self, items: List[int], items_utilities: List[int], transaction_utility: int) -> None:
        self.items = items
        self.itemsUtilities = items_utilities
        self.transactionUtility = transaction_utility

    def getItems(self) -> List[int]:
        return self.items

    def get(self, index: int) -> int:
        return self.items[index]

    def print(self) -> None:
        print(self.to_string(), end="")

    def to_string(self) -> str:
        items_part = " ".join(str(item) for item in self.items)
        utilities_part = " ".join(str(u) for u in self.itemsUtilities)
        return f"{items_part}:{self.transactionUtility}: {utilities_part} "

    def contains(self, item: int) -> bool:
        for item_i in self.items:
            if item_i == item:
                return True
            if item_i > item:
                return False
        return False

    def size(self) -> int:
        return len(self.items)

    def getItemsUtilities(self) -> List[int]:
        return self.itemsUtilities

    def getTransactionUtility(self) -> int:
        return self.transactionUtility


class UtilityTransactionDatabaseTP:
    def __init__(self) -> None:
        self.allItems: Set[int] = set()
        self.transactions: List[TransactionTP] = []

    def addTransaction(self, transaction: TransactionTP) -> None:
        self.transactions.append(transaction)
        self.allItems.update(transaction.getItems())

    def load_file(self, path: str) -> None:
        try:
            with open(path, "r", encoding="utf-8") as my_input:
                for this_line in my_input:
                    this_line = this_line.strip()
                    if (
                        this_line == ""
                        or this_line[0] == "#"
                        or this_line[0] == "%"
                        or this_line[0] == "@"
                    ):
                        continue
                    self.process_transaction(this_line.split(":"))
        except Exception:
            import traceback

            traceback.print_exc()

    def process_transaction(self, line: List[str]) -> None:
        transaction_utility = int(line[1])

        items = [int(item) for item in line[0].split(" ")]
        items_utilities = [int(utility) for utility in line[2].split(" ")]

        self.bubble_sort(items, items_utilities)
        self.transactions.append(TransactionTP(items, items_utilities, transaction_utility))

    def bubble_sort(self, items: List[int], items_utilities: List[int]) -> None:
        for i in range(len(items)):
            for j in range(len(items) - 1, i, -1):
                if items[j] < items[j - 1]:
                    items[j], items[j - 1] = items[j - 1], items[j]
                    items_utilities[j], items_utilities[j - 1] = (
                        items_utilities[j - 1],
                        items_utilities[j],
                    )

    def print_database(self) -> None:
        print("===================  Database ===================")
        count = 0
        for itemset in self.transactions:
            print(f"0{count}:  ", end="")
            itemset.print()
            print("")
            count += 1

    def size(self) -> int:
        return len(self.transactions)

    def getTransactions(self) -> List[TransactionTP]:
        return self.transactions

    def getAllItems(self) -> Set[int]:
        return self.allItems


if __name__ == "__main__":
    MainTestCHUD.main()
