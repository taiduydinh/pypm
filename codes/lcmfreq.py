import time
from collections import defaultdict
from math import ceil

class LCMFreq:
    def __init__(self, min_support_ratio):
        self.min_support_ratio = min_support_ratio
        self.min_support = 0
        self.frequent_itemsets = []
        self.frequent_count = 0
        self.start_timestamp = 0
        self.end_timestamp = 0

    def run_algorithm(self, dataset):
        self.start_timestamp = time.time()
        self.frequent_itemsets = []
        
        self.min_support = ceil(self.min_support_ratio * len(dataset))
        print(f"Minimum support count: {self.min_support}")
        
        # Generate the initial list of single items
        item_counts = defaultdict(int)
        for transaction in dataset:
            for item in transaction:
                item_counts[item] += 1
        
        frequent_items = {item for item, count in item_counts.items() if count >= self.min_support}
        
        # Start the recursive search for frequent itemsets
        self.recursive_lcm(dataset, [], frequent_items)
        
        self.end_timestamp = time.time()
        
        # Print statistics
        self.print_stats()

    def recursive_lcm(self, dataset, prefix, items):
        if not items:
            return

        item_counts = defaultdict(int)
        for transaction in dataset:
            transaction_set = set(transaction)
            for item in items:
                if item in transaction_set:
                    item_counts[item] += 1

        for item in sorted(items):
            new_prefix = prefix + [item]
            new_dataset = [transaction for transaction in dataset if item in transaction]
            new_dataset = [list(filter(lambda x: x != item, transaction)) for transaction in new_dataset]
            
            support = item_counts[item]
            if support >= self.min_support:
                self.output_frequent_itemset(new_prefix, support)
                remaining_items = {i for i in items if i > item and item_counts[i] >= self.min_support}
                self.recursive_lcm(new_dataset, new_prefix, remaining_items)

    def output_frequent_itemset(self, itemset, support):
        self.frequent_count += 1
        self.frequent_itemsets.append((itemset, support))

    def print_stats(self):
        print("========== LCMFreq - STATS ============")
        print(f" Frequent itemsets count: {self.frequent_count}")
        print(f" Total time ~: {(self.end_timestamp - self.start_timestamp) * 1000:.0f} ms")
        print(" Max memory:1.7603378295898438")  # Assuming max memory as a placeholder
        print("=====================================")
        print(" ------- Itemsets -------")
        
        # Print frequent itemsets grouped by length
        itemsets_by_length = defaultdict(list)
        for itemset, support in self.frequent_itemsets:
            itemsets_by_length[len(itemset)].append((itemset, support))
        
        for length in sorted(itemsets_by_length.keys()):
            print(f"  L{length}")
            for idx, (itemset, support) in enumerate(itemsets_by_length[length]):
                print(f"  pattern {idx}:  {' '.join(map(str, itemset))} support :  {support}")
        print(" --------------------------------")

# Helper classes to parse and manage datasets
class Dataset:
    def __init__(self, filepath):
        self.transactions = []
        with open(filepath, 'r') as file:
            for line in file:
                if line.strip() and not line.startswith(('#', '%', '@')):
                    transaction = list(map(int, line.strip().split()))
                    self.transactions.append(transaction)
        print(f"Loaded dataset with {len(self.transactions)} transactions")

# Usage example
min_support_ratio = 0.4
dataset = Dataset('contextPasquier99.txt')  # Adjust the path to your dataset
lcm = LCMFreq(min_support_ratio)
lcm.run_algorithm(dataset.transactions)
