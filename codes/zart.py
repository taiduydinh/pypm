import math
import time
import os
from typing import List, Dict

# ------------------------------------------------------------
# MemoryLogger
# ------------------------------------------------------------
class MemoryLogger:
    """Equivalent to MemoryLogger.java"""
    _instance = None

    def __init__(self):
        self.max_memory = 0.0

    @staticmethod
    def get_instance():
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def reset(self):
        self.max_memory = 0.0

    def check_memory(self):
        import tracemalloc
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / 1024 / 1024
        if current_mb > self.max_memory:
            self.max_memory = current_mb
        return current_mb

    def get_max_memory(self):
        return self.max_memory


# ------------------------------------------------------------
# AbstractItemset
# ------------------------------------------------------------
class AbstractItemset:
    """Equivalent to AbstractItemset.java"""
    def size(self) -> int:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError

    def get_absolute_support(self) -> int:
        raise NotImplementedError

    def get_relative_support(self, nb_object: int) -> float:
        raise NotImplementedError

    def contains(self, item: int) -> bool:
        raise NotImplementedError

    def get_relative_support_as_string(self, nb_object: int) -> str:
        freq = self.get_relative_support(nb_object)
        return f"{freq:.5f}"


# ------------------------------------------------------------
# AbstractOrderedItemset
# ------------------------------------------------------------
class AbstractOrderedItemset(AbstractItemset):
    """Equivalent to AbstractOrderedItemset.java"""

    def get(self, position: int) -> int:
        raise NotImplementedError

    def get_last_item(self) -> int:
        return self.get(self.size() - 1)

    def __str__(self):
        if self.size() == 0:
            return "EMPTYSET"
        return " ".join(str(self.get(i)) for i in range(self.size()))

    def get_relative_support(self, nb_object: int) -> float:
        return self.get_absolute_support() / float(nb_object)

    def contains(self, item: int) -> bool:
        for i in range(self.size()):
            if self.get(i) == item:
                return True
            elif self.get(i) > item:
                return False
        return False

    def is_equal_to(self, itemset2: "AbstractOrderedItemset") -> bool:
        if self.size() != itemset2.size():
            return False
        return all(self.get(i) == itemset2.get(i) for i in range(self.size()))

    def all_the_same_except_last_item(self, itemset2: "AbstractOrderedItemset"):
        if itemset2.size() != self.size():
            return None
        for i in range(self.size()):
            if i == self.size() - 1:
                if self.get(i) >= itemset2.get(i):
                    return None
            elif self.get(i) != itemset2.get(i):
                return None
        return itemset2.get(itemset2.size() - 1)


# ------------------------------------------------------------
# ArraysAlgos
# ------------------------------------------------------------
class ArraysAlgos:
    """Helper utilities"""

    @staticmethod
    def intersect_two_sorted_arrays(array1: List[int], array2: List[int]) -> List[int]:
        i, j = 0, 0
        intersection = []
        while i < len(array1) and j < len(array2):
            if array1[i] == array2[j]:
                intersection.append(array1[i])
                i += 1
                j += 1
            elif array1[i] < array2[j]:
                i += 1
            else:
                j += 1
        return intersection


# ------------------------------------------------------------
# Itemset
# ------------------------------------------------------------
class Itemset(AbstractOrderedItemset):
    """Equivalent to Itemset.java"""

    def __init__(self, items=None, support: int = 0):
        if items is None:
            self.itemset = []
        elif isinstance(items, int):
            self.itemset = [items]
        elif isinstance(items, list):
            self.itemset = items
        else:
            self.itemset = list(items)
        self.support = support

    def get_items(self) -> List[int]:
        return self.itemset

    def get_absolute_support(self) -> int:
        return self.support

    def size(self) -> int:
        return len(self.itemset)

    def get(self, position: int) -> int:
        return self.itemset[position]

    def set_absolute_support(self, support: int):
        self.support = support

    def increase_transaction_count(self):
        self.support += 1

    def clone_itemset_minus_one_item(self, item_to_remove: int) -> "Itemset":
        new_items = [x for x in self.itemset if x != item_to_remove]
        return Itemset(new_items)

    def intersection(self, itemset2: "Itemset") -> "Itemset":
        inter = ArraysAlgos.intersect_two_sorted_arrays(self.itemset, itemset2.itemset)
        return Itemset(inter)

    def __hash__(self):
        return hash(tuple(self.itemset))

    def __eq__(self, other):
        if not isinstance(other, Itemset):
            return False
        return self.itemset == other.itemset


# ------------------------------------------------------------
# TransactionDatabase
# ------------------------------------------------------------
class TransactionDatabase:
    """Equivalent to TransactionDatabase.java"""

    def __init__(self):
        self.items = set()
        self.transactions: List[List[int]] = []

    def add_transaction(self, transaction: List[int]):
        self.transactions.append(transaction)
        self.items.update(transaction)

    def load_file(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ("#", "%", "@"):
                    continue
                parts = [int(x) for x in line.split()]
                self.add_transaction(parts)

    def size(self) -> int:
        return len(self.transactions)

    def get_transactions(self) -> List[List[int]]:
        return self.transactions


# ------------------------------------------------------------
# TCTableCandidate
# ------------------------------------------------------------
class TCTableCandidate:
    """Equivalent to TCTableCandidate.java"""

    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.map_pred_supp: Dict[Itemset, int] = {}
        self.map_key: Dict[Itemset, bool] = {}

    def there_is_a_row_key_value_true(self, i: int) -> bool:
        for c in self.levels[i]:
            if self.map_key.get(c, False):
                return True
        return False


# ------------------------------------------------------------
# TFTableFrequent
# ------------------------------------------------------------
class TFTableFrequent:
    """Equivalent to TFTableFrequent.java"""

    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.map_pred_supp: Dict[Itemset, int] = {}
        self.map_key: Dict[Itemset, bool] = {}
        self.map_closed: Dict[Itemset, bool] = {}

    def add_frequent_itemset(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        self.levels[itemset.size()].append(itemset)

    def get_level_for_zart(self, i: int) -> List[Itemset]:
        if i + 1 == len(self.levels):
            new_list = []
            self.levels.append(new_list)
            return new_list
        return self.levels[i + 1]


# ------------------------------------------------------------
# TZTableClosed
# ------------------------------------------------------------
class TZTableClosed:
    """Equivalent to TZTableClosed.java"""

    def __init__(self):
        self.levels: List[List[Itemset]] = []
        self.map_generators: Dict[Itemset, List[Itemset]] = {}

    def add_closed_itemset(self, itemset: Itemset):
        while len(self.levels) <= itemset.size():
            self.levels.append([])
        self.levels[itemset.size()].append(itemset)

    def get_level_for_zart(self, i: int) -> List[Itemset]:
        if i + 1 == len(self.levels):
            new_list = []
            self.levels.append(new_list)
            return new_list
        return self.levels[i + 1]


# ------------------------------------------------------------
# AlgoZart (main algorithm)
# ------------------------------------------------------------
class AlgoZart:
    """Equivalent to AlgoZart.java"""

    def __init__(self):
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.minsup_relative = 0
        self.context: TransactionDatabase = None
        self.table_closed: TZTableClosed = None
        self.table_frequent: TFTableFrequent = None
        self.table_candidate: TCTableCandidate = None
        self.frequent_generators_fg: List[Itemset] = []

    def run_algorithm(self, database: TransactionDatabase, minsupp: float) -> TZTableClosed:
        self.start_timestamp = time.time()
        MemoryLogger.get_instance().reset()

        self.context = database
        self.frequent_generators_fg = []
        self.table_closed = TZTableClosed()
        self.table_frequent = TFTableFrequent()
        self.table_candidate = TCTableCandidate()

        self.minsup_relative = math.ceil(minsupp * database.size())
        print(f"[DEBUG] minsupRelative = {self.minsup_relative}, total_transactions = {database.size()}")

        # 1. Count item supports
        map_item_support: Dict[int, int] = {}
        for transaction in database.get_transactions():
            for item in transaction:
                map_item_support[item] = map_item_support.get(item, 0) + 1

        # 2. Remove infrequent items
        for transaction in database.get_transactions():
            transaction[:] = [it for it in transaction if map_item_support[it] >= self.minsup_relative]

        # 3. Initialize 1-itemsets
        self.table_candidate.levels.append([])
        for item, count in map_item_support.items():
            itemset = Itemset([item])
            itemset.set_absolute_support(count)
            if count >= self.minsup_relative:
                self.table_frequent.add_frequent_itemset(itemset)
                self.table_candidate.levels[0].append(itemset)

        if len(self.table_frequent.levels) != 0:
            full_column = False
            for l in self.table_frequent.get_level_for_zart(0):
                self.table_frequent.map_closed[l] = True
                if l.get_absolute_support() == database.size():
                    self.table_frequent.map_key[l] = False
                    full_column = True
                else:
                    self.table_frequent.map_key[l] = True

            emptyset = Itemset([])
            if full_column:
                self.frequent_generators_fg.append(emptyset)
            else:
                self.table_frequent.add_frequent_itemset(emptyset)
                self.table_frequent.map_closed[emptyset] = True
                self.table_frequent.map_pred_supp[emptyset] = database.size()
                self.table_closed.add_closed_itemset(emptyset)
                self.table_closed.map_generators[emptyset] = []
                emptyset.set_absolute_support(database.size())

            i = 1
            while True:
                self.zart_gen(i)
                if len(self.table_candidate.levels[i]) == 0:
                    break

                if self.table_candidate.there_is_a_row_key_value_true(i):
                    for o in database.get_transactions():
                        for s in self.subset(self.table_candidate.levels[i], o):
                            if self.table_candidate.map_key.get(s, False):
                                s.increase_transaction_count()

                for c in self.table_candidate.levels[i]:
                    if c.get_absolute_support() >= self.minsup_relative:
                        if self.table_candidate.map_key.get(c, False) and \
                           c.get_absolute_support() == self.table_candidate.map_pred_supp.get(c, 0):
                            self.table_candidate.map_key[c] = False
                        self.table_frequent.add_frequent_itemset(c)
                        self.table_frequent.map_key[c] = self.table_candidate.map_key.get(c, False)
                        self.table_frequent.map_pred_supp[c] = self.table_candidate.map_pred_supp.get(c, 0)

                for l in self.table_frequent.get_level_for_zart(i):
                    self.table_frequent.map_closed[l] = True
                    for s in self.subset(self.table_frequent.get_level_for_zart(i - 1), l):
                        if s.get_absolute_support() == l.get_absolute_support():
                            self.table_frequent.map_closed[s] = False

                self.table_closed.levels.append([])
                for l in self.table_frequent.get_level_for_zart(i - 1):
                    if self.table_frequent.map_closed.get(l, False):
                        self.table_closed.get_level_for_zart(i - 1).append(l)

                self.find_generators(self.table_closed.get_level_for_zart(i - 1), i)
                MemoryLogger.get_instance().check_memory()
                i += 1

            self.table_closed.levels.append([])
            for l in self.table_frequent.get_level_for_zart(i - 1):
                self.table_closed.get_level_for_zart(i - 1).append(l)
            self.find_generators(self.table_closed.get_level_for_zart(i - 1), i)

        MemoryLogger.get_instance().check_memory()
        self.end_timestamp = time.time()
        return self.table_closed

    # --------------------------------------------------------
    def subset(self, s: List[Itemset], l) -> List[Itemset]:
        """Return list of itemsets from list s that are subsets of l.
        l can be either a List[int] (transaction) or an Itemset."""
        retour = []
        if isinstance(l, Itemset):
            items_l = l.itemset
        else:
            items_l = l
        for itemset_s in s:
            all_included = True
            for it in itemset_s.itemset:
                if it not in items_l:
                    all_included = False
                    break
            if all_included:
                retour.append(itemset_s)
        return retour

    # --------------------------------------------------------
    def find_generators(self, zi: List[Itemset], i: int):
        for z in zi:
            s = self.subset(self.frequent_generators_fg, z.itemset)
            self.table_closed.map_generators[z] = s
            for x in s:
                if x in self.frequent_generators_fg:
                    self.frequent_generators_fg.remove(x)
        for l in self.table_frequent.get_level_for_zart(i - 1):
            if self.table_frequent.map_key.get(l, False) and not self.table_frequent.map_closed.get(l, False):
                self.frequent_generators_fg.append(l)

    def get_previous_occurrence(self, itemset: Itemset, list_itemsets: List[Itemset]):
        for itemset2 in list_itemsets:
            if itemset2 == itemset:
                return itemset2
        return None

    def zart_gen(self, i: int):
        self.prepare_candidate_size_i(i)
        for c in list(self.table_candidate.levels[i]):
            self.table_candidate.map_key[c] = True
            self.table_candidate.map_pred_supp[c] = self.context.size() + 1

            for j in range(c.size()):
                s = c.clone_itemset_minus_one_item(c.get(j))
                found = False
                for itemset2 in self.table_frequent.get_level_for_zart(i - 1):
                    if itemset2 == s:
                        found = True
                        break
                if not found:
                    self.table_candidate.levels[i].remove(c)
                    break
                else:
                    occur = self.get_previous_occurrence(s, self.table_candidate.levels[i - 1])
                    if occur and occur.get_absolute_support() < self.table_candidate.map_pred_supp[c]:
                        self.table_candidate.map_pred_supp[c] = occur.get_absolute_support()
                    if occur and not self.table_frequent.map_key.get(occur, True):
                        self.table_candidate.map_key[c] = False

            if not self.table_candidate.map_key.get(c, True):
                c.set_absolute_support(self.table_candidate.map_pred_supp.get(c, 0))

    def prepare_candidate_size_i(self, size: int):
        self.table_candidate.levels.append([])
        for itemset1 in self.table_frequent.get_level_for_zart(size - 1):
            for itemset2 in self.table_frequent.get_level_for_zart(size - 1):
                missing = itemset2.all_the_same_except_last_item(itemset1)
                if missing is not None:
                    union = itemset2.itemset[:] + [missing]
                    self.table_candidate.levels[size].append(Itemset(union))

    def get_table_frequent(self):
        return self.table_frequent

    def print_statistics(self):
        print("========== ZART - STATS ============")
        print(f" Total time ~: {(self.end_timestamp - self.start_timestamp) * 1000:.0f} ms")
        print(f" Max memory: {MemoryLogger.get_instance().get_max_memory():.2f} MB")
        print("=====================================")

    def save_results_to_file(self, output: str):
        with open(output, "w", encoding="utf-8") as writer:
            writer.write("======= List of closed itemsets and their generators ============\n")
            for i, level in enumerate(self.table_closed.levels):
                for closed in level:
                    writer.write(f" CLOSED : \n   {closed} #SUP: {closed.get_absolute_support()}\n")
                    writer.write("   GENERATOR(S) :\n")
                    generators = self.table_closed.map_generators.get(closed, [])
                    if not generators:
                        writer.write(f"    {closed}\n")
                    else:
                        for gen in generators:
                            writer.write(f"     {gen}\n")

            writer.write("======= List of frequent itemsets ============\n")
            for i, level in enumerate(self.table_frequent.levels):
                for itemset in level:
                    writer.write(f" ITEMSET : {itemset} #SUP: {itemset.get_absolute_support()}\n")


# ------------------------------------------------------------
# MainTestZart_saveToFile
# ------------------------------------------------------------
class MainTestZartSaveToFile:
    """Equivalent to MainTestZart_saveToFile.java"""

    @staticmethod
    def file_to_path(filename: str) -> str:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_dir, filename)

    @staticmethod
    def main():
        input_path = MainTestZartSaveToFile.file_to_path("contextZart.txt")
        output_path = MainTestZartSaveToFile.file_to_path("Zart_outputs.txt")

        context = TransactionDatabase()
        context.load_file(input_path)

        minsup = 0.4 
        zart = AlgoZart()
        results = zart.run_algorithm(context, minsup)
        frequents = zart.get_table_frequent()
        zart.print_statistics()
        zart.save_results_to_file(output_path)

        print(f"\n Mining complete! Output saved to: {output_path}")
        print("=========================================")


# ------------------------------------------------------------
# Script entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    MainTestZartSaveToFile.main()
