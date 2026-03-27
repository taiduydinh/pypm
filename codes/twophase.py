import time
import decimal
from decimal import Decimal
from typing import List, Set, Tuple
from itertools import combinations

class ItemsetTP:
    def __init__(self):
        self.items = []
        self.utility = 0
        self.transactions_ids = set()

    def get_relative_support(self, nb_object: int) -> float:
        return len(self.transactions_ids) / nb_object

    def get_relative_support_as_string(self, nb_object: int) -> str:
        frequency = len(self.transactions_ids) / nb_object
        return f"{frequency:.4f}"

    def get_absolute_support(self) -> int:
        return len(self.transactions_ids)

    def add_item(self, value: int):
        self.items.append(value)

    def get_items(self) -> List[int]:
        return self.items

    def __getitem__(self, index: int) -> int:
        return self.items[index]

    def print(self):
        print(self.__str__())

    def __str__(self):
        return ' '.join(str(item) for item in self.items)

    def set_tidset(self, list_transaction_ids: Set[int]):
        self.transactions_ids = list_transaction_ids

    def size(self) -> int:
        return len(self.items)

    def get_tidset(self) -> Set[int]:
        return self.transactions_ids

    def get_utility(self) -> int:
        return self.utility

    def increment_utility(self, increment: int):
        self.utility += increment


class ItemsetsTP:
    def __init__(self, name: str):
        self.levels = [[]]
        self.itemsets_count = 0
        self.name = name

    def print_itemsets(self, transaction_count: int):
        print(f" ------- {self.name} -------")
        pattern_count = 0
        for level_index, level in enumerate(self.levels):
            print(f"  L{level_index} ")
            for itemset in level:
                print(f"  pattern {pattern_count}  ", end="")
                itemset.print()
                print(f" #SUP: {itemset.get_absolute_support()} #UTIL: {itemset.get_utility()}")
                pattern_count += 1
        print(" --------------------------------")

    def save_results_to_file(self, output: str, transaction_count: int):
        with open(output, 'w') as writer:
            for level in self.levels:
                for itemset in level:
                    writer.write(f"{str(itemset)} #SUP: {itemset.get_relative_support(transaction_count):.4f} #UTIL: {itemset.get_utility()}\n")

    def add_itemset(self, itemset: ItemsetTP, k: int):
        while len(self.levels) <= k:
            self.levels.append([])
        self.levels[k].append(itemset)
        self.itemsets_count += 1

    def get_levels(self) -> List[List[ItemsetTP]]:
        return self.levels

    def get_itemsets_count(self) -> int:
        return self.itemsets_count

    def decrease_count(self):
        self.itemsets_count -= 1


class TransactionTP:
    def __init__(self, items_utilities: List[Tuple[int, int]], transaction_utility: int):
        self.items_utilities = items_utilities
        self.transaction_utility = transaction_utility

    def print(self):
        print(self.__str__())

    def __str__(self):
        items_str = ' '.join(f"{item} {utility}" for item, utility in self.items_utilities)
        return f"{items_str} : {self.transaction_utility}"

    def contains(self, item: int) -> bool:
        return any(item_util[0] == item for item_util in self.items_utilities)

    def size(self) -> int:
        return len(self.items_utilities)

    def get_items_utilities(self) -> List[Tuple[int, int]]:
        return self.items_utilities

    def get_transaction_utility(self) -> int:
        return self.transaction_utility


class UtilityTransactionDatabaseTP:
    def __init__(self):
        self.all_items = set()
        self.transactions = []

    def load_file(self, path: str):
        with open(path, 'r') as file:
            for line in file:
                if line.startswith(('#', '%', '@')) or not line.strip():
                    continue
                parts = line.split(':')
                items = list(map(int, parts[0].split()))
                transaction_utility = int(parts[1].strip())
                utilities = list(map(int, parts[2].split()))
                item_utilities = list(zip(items, utilities))
                self.transactions.append(TransactionTP(item_utilities, transaction_utility))
                self.all_items.update(items)

    def print_database(self):
        print("===================  Database ===================")
        for index, transaction in enumerate(self.transactions):
            print(f"{index:02}:  ", end="")
            transaction.print()

    def size(self) -> int:
        return len(self.transactions)

    def get_transactions(self) -> List[TransactionTP]:
        return self.transactions

    def get_all_items(self) -> Set[int]:
        return self.all_items

class AlgoTwoPhase:
    def __init__(self):
        self.highUtilityItemsets = ItemsetsTP("HIGH UTILITY ITEMSETS")
        self.database = None
        self.minUtility = 0
        self.startTimestamp = 0
        self.endTimestamp = 0
        self.candidatesCount = 0

    def run_algorithm(self, database, minUtility):
        self.database = database
        self.minUtility = minUtility
        self.startTimestamp = time.time()

        # Phase 1: Generate candidates
        candidates = self.generate_candidates(database, minUtility)

        # Phase 2: Calculate exact utility and filter candidates
        self.filter_candidates(candidates, minUtility)

        self.endTimestamp = time.time()

        # Save the results to a file
        self.highUtilityItemsets.save_results_to_file("output.txt", self.database.size())

        return self.highUtilityItemsets

    def generate_candidates(self, database, minUtility):
        itemTWU = {}
        for transaction in database.get_transactions():
            unique_items = set(item for item, _ in transaction.get_items_utilities())
            for item in unique_items:
                if item not in itemTWU:
                    itemTWU[item] = 0
                itemTWU[item] += transaction.get_transaction_utility()

        candidates = []
        all_items = list(itemTWU.keys())
        for size in range(1, len(all_items) + 1):
            for subset in combinations(all_items, size):
                if sum(itemTWU[item] for item in subset) >= minUtility:
                    candidate = ItemsetTP()
                    for item in subset:
                        candidate.add_item(item)
                    candidates.append(candidate)
        self.candidatesCount = len(candidates)  # Update the candidates count
        return candidates

    def filter_candidates(self, candidates, minUtility):
        for candidate in candidates:
            utility = 0
            transaction_ids = set()
            for idx, transaction in enumerate(self.database.get_transactions()):
                transaction_items = set(item for item, _ in transaction.get_items_utilities())
                candidate_items = set(candidate.get_items())
                if candidate_items <= transaction_items:
                    utility += sum(ut for it, ut in transaction.get_items_utilities() if it in candidate_items)
                    transaction_ids.add(idx)
            if utility >= minUtility:
                candidate.set_tidset(transaction_ids)
                candidate.increment_utility(utility)
                self.highUtilityItemsets.add_itemset(candidate, candidate.size())
            else:
                self.candidatesCount -= 1

    def print_stats(self):
        print("============= TWO-PHASE ALGORITHM - STATS =============")
        print(" Transactions count from database : ", len(self.database.get_transactions()))
        print(" Candidates count : ", self.candidatesCount)
        print(" High-utility itemsets count : ", self.highUtilityItemsets.get_itemsets_count())
        print(" Total time ~ ", round((self.endTimestamp - self.startTimestamp) * 1000), "ms")
        print("===================================================")

# Usage
database = UtilityTransactionDatabaseTP()
database.load_file('DB_Utility.txt')
algo = AlgoTwoPhase()
high_utility_itemsets = algo.run_algorithm(database, minUtility=30)
algo.print_stats()  # Ensure to print stats after the algorithm run
