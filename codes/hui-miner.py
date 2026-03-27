import time
import psutil
import os
import itertools

class Transaction:
    def __init__(self, items, utilities):
        self.items = items
        self.utilities = utilities

def parse_data(file_path):
    transactions = []
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(":")
            items = list(map(int, parts[0].split()))
            utilities = list(map(int, parts[2].split()))
            transactions.append(Transaction(items, utilities))
    return transactions

def calculate_utility(itemset, transactions):
    utility = 0
    for transaction in transactions:
        if all(item in transaction.items for item in itemset):
            indices = [transaction.items.index(item) for item in itemset]
            utility += sum(transaction.utilities[index] for index in indices)
    return utility

def generate_candidates(items):
    return [itemset for i in range(1, len(items) + 1) for itemset in itertools.combinations(items, i)]

def hui_miner(transactions, min_utility):
    items = sorted(set(itertools.chain.from_iterable(transaction.items for transaction in transactions)))
    candidates = generate_candidates(items)
    high_utility_itemsets = []
    
    join_count = 0

    for itemset in candidates:
        join_count += 1
        utility = calculate_utility(itemset, transactions)
        if utility >= min_utility:
            high_utility_itemsets.append((itemset, utility))
    
    return high_utility_itemsets, join_count

# Read data from dataset.txt
file_path = "DB_Utility.txt"
transactions = parse_data(file_path)

# Define minimum utility threshold
min_utility = 30

# Start time
start_time = time.time()

# Run HUI-Miner algorithm
high_utility_itemsets, join_count = hui_miner(transactions, min_utility)

# End time
end_time = time.time()
total_time = (end_time - start_time) * 1000  # convert to milliseconds

# Memory usage
process = psutil.Process(os.getpid())
memory_usage = process.memory_info().rss / 1024 / 1024  # convert to MB

# Print stats
print("=============  HUI-MINER ALGORITHM - STATS =============")
print(f" Total time ~ {total_time:.2f} ms")
print(f" Memory ~ {memory_usage:.2f} MB")
print(f" High-utility itemsets count : {len(high_utility_itemsets)}")
print(f" Join count : {join_count}")
print("===================================================")

# Write to output.txt
with open("output.txt", "w") as file:
    for itemset, utility in high_utility_itemsets:
        file.write(f"{' '.join(map(str, itemset))} #UTIL: {utility}\n")

# Print results
for itemset, utility in high_utility_itemsets:
    print(f"{' '.join(map(str, itemset))} #UTIL: {utility}")
