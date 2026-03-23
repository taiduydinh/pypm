import itertools

# ===============================
# CHANGE THESE VALUES
# ===============================
input_file = "105_TKU-CE+/ca/pfv/spmf/test/DB_Utility.txt"
output_file = "105_TKU-CE+/ca/pfv/spmf/test/output.txt"
k = 7  # Top-K value
# ===============================


# ===============================
# STEP 1: READ DATABASE FROM FILE
# ===============================
database = []

with open(input_file, "r") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith(("#", "%", "@")):
            continue

        items_part, trans_util, utils_part = line.split(":")
        items = list(map(int, items_part.split()))
        utils = list(map(int, utils_part.split()))

        transaction = {}
        for i in range(len(items)):
            transaction[items[i]] = utils[i]

        database.append(transaction)


# ===============================
# STEP 2: GET ALL UNIQUE ITEMS
# ===============================
all_items = sorted(set(
    item for transaction in database for item in transaction.keys()
))


# ===============================
# STEP 3: COMPUTE UTILITY OF ITEMSET
# ===============================
def compute_utility(itemset):
    total_utility = 0

    for transaction in database:
        if all(item in transaction for item in itemset):
            total_utility += sum(transaction[item] for item in itemset)

    return total_utility


# ===============================
# STEP 4: GENERATE ALL ITEMSETS
# ===============================
results = []

for r in range(1, len(all_items) + 1):
    for combination in itertools.combinations(all_items, r):
        util = compute_utility(combination)
        if util > 0:
            results.append((combination, util))


# ===============================
# STEP 5: SORT AND GET TOP-K
# ===============================
results.sort(key=lambda x: x[1], reverse=True)
top_k = results[:k]


# ===============================
# STEP 6: PRINT OUTPUT (Same Format)
# ===============================
with open(output_file, "w") as f:
    for itemset, utility in top_k:
        items_str = " ".join(map(str, itemset))
        line = f"{items_str} #UTIL:{utility}"
        f.write(line + "\n")
        print("Results saved to output.txt")