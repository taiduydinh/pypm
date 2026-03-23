import time
import os
import math
import sys

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# -----------------------------------------------------------
# Load transaction database
# -----------------------------------------------------------
def load_transactions(path):
    transactions = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items = set(int(x) for x in line.split())
            transactions.append(items)
    return transactions


# -----------------------------------------------------------
# Compute support of an itemset
# -----------------------------------------------------------
def support(itemset, transactions):
    return sum(1 for t in transactions if itemset.issubset(t))


# -----------------------------------------------------------
# Generate all frequent itemsets (Standard Apriori algorithm)
# -----------------------------------------------------------
def generate_all_frequent_itemsets(transactions, minsup):
    all_items = sorted({item for t in transactions for item in t})
    frequent_itemsets = []

    # Generate singletons
    current_level = []
    for item in all_items:
        s = support({item}, transactions)
        if s >= minsup:
            current_level.append(({item}, s))

    frequent_itemsets.extend(current_level)

    # Generate larger itemsets using Apriori principle
    while current_level:
        # Generate candidates of size k+1 from itemsets of size k
        candidates = []
        current_level_list = [itemset for itemset, _ in current_level]
        
        # Join step: for itemsets of size k, join those with same first k-1 items
        for i in range(len(current_level_list)):
            for j in range(i + 1, len(current_level_list)):
                itemset_i = sorted(list(current_level_list[i]))
                itemset_j = sorted(list(current_level_list[j]))
                
                # Check if first k-1 items are the same
                if itemset_i[:-1] == itemset_j[:-1]:
                    # Generate candidate by combining
                    candidate = set(itemset_i) | set(itemset_j)
                    candidates.append(candidate)
        
        # Remove duplicates
        candidates = list({frozenset(c): c for c in candidates}.values())
        
        # Support counting: count support for each candidate
        new_level = []
        for candidate in candidates:
            s = support(candidate, transactions)
            if s >= minsup:
                new_level.append((candidate, s))
        
        if not new_level:
            break
        
        frequent_itemsets.extend(new_level)
        current_level = new_level

    return frequent_itemsets


# -----------------------------------------------------------
# Filter to keep only closed itemsets
# -----------------------------------------------------------
def filter_closed_itemsets(frequent_itemsets):
    closed = []
    
    for itemset_x, sup_x in frequent_itemsets:
        is_closed = True
        
        # Check if there exists a superset with same support
        for itemset_y, sup_y in frequent_itemsets:
            if sup_x == sup_y and itemset_x < itemset_y:  # itemset_x is proper subset of itemset_y
                is_closed = False
                break
        
        if is_closed:
            closed.append((itemset_x, sup_x))
    
    return closed


# -----------------------------------------------------------
# Write result in Java-style format
# -----------------------------------------------------------
def write_output(pairs, filename):
    with open(filename, "w") as f:
        # Sort by: size first (ascending), then by items (ascending)
        sorted_pairs = sorted(pairs, key=lambda x: (len(x[0]), sorted(x[0])))
        for (items, sup) in sorted_pairs:
            line = " ".join(str(x) for x in sorted(items))
            f.write(f"{line}  #SUP: {sup}\n")


# -----------------------------------------------------------
# Main runner
# -----------------------------------------------------------
def main():
    input_path = os.path.join(SCRIPT_DIR, "contextPasquier99.txt")
    output_path = os.path.join(SCRIPT_DIR, "charm_outputs.txt")
    
    # Get minsup from command line argument or use default
    if len(sys.argv) > 1:
        try:
            minsup_relative = float(sys.argv[1])
        except ValueError:
            print(f"Error: Invalid minsup value '{sys.argv[1]}'. Using default 0.1")
            minsup_relative = 0.1
    else:
        minsup_relative = 0.1  # Default value
    
    print("Loading transactions...")
    transactions = load_transactions(input_path)
    
    # Convert relative minsup to absolute count (rounded up)
    minsup_absolute = math.ceil(minsup_relative * len(transactions))
    
    print(f"Running CHARM (finding frequent itemsets)...")
    print(f"Relative minsup: {minsup_relative} ({minsup_relative*100}%)")
    print(f"Absolute minsup: {minsup_absolute} transactions")
    
    start = time.time()
    all_frequent = generate_all_frequent_itemsets(transactions, minsup_absolute)
    
    print("Filtering closed itemsets...")
    pairs = filter_closed_itemsets(all_frequent)
    end = time.time()

    write_output(pairs, output_path)

    print("==== PYTHON CHARM OUTPUT ====")
    print(f"Transactions: {len(transactions)}")
    print(f"Frequent closed itemsets: {len(pairs)}")
    print(f"Time: {int((end-start)*1000)} ms")
    print(f"Output written to: {output_path}")



if __name__ == "__main__":
    main()
