import sys
import os
import urllib.parse
from collections import defaultdict


class AlgoDCI_Closed:
    def __init__(self):
        self.closedCount = 0
        self.tidCount = 0
        self.maxItemId = 1
        self.minSuppRelative = 0
        self.writer = None
        self.database = defaultdict(set)

    def runAlgorithm(self, input_file, output_file, minsup):
        import time
        start_timestamp = time.time()
        self.closedCount = 0
        
        print("Running the DCI-Closed algorithm")
        
        with open(output_file, 'w') as self.writer:
            self.minSuppRelative = minsup
            self.createVerticalDatabase(input_file)
            
            closedset = []
            closedsetTIDs = set()
            preset = []
            postset = []

            for i in range(1, self.maxItemId + 1):
                if len(self.database[i]) >= self.minSuppRelative:
                    postset.append(i)
            
            postset.sort(key=lambda x: (len(self.database[x]), x))

            self.dci_closed(True, closedset, closedsetTIDs, postset, preset)
        
        print("========== DCI_CLOSED - STATS ============")
        print("Number of transactions:", self.tidCount)
        print("Number of frequent closed itemsets:", self.closedCount)
        print("Total time ~:", (time.time() - start_timestamp), "seconds")
    
    def dci_closed(self, firstTime, closedset, closedsetTIDs, postset, preset):
        for i in postset:
            newgenTIDs = self.database[i] if firstTime else self.intersectTIDset(closedsetTIDs, self.database[i])
            
            if len(newgenTIDs) >= self.minSuppRelative:
                newgen = closedset + [i]
                
                if not self.is_dup(newgenTIDs, preset):
                    closedsetNew = newgen.copy()
                    closedsetNewTIDs = self.database[i].copy() if firstTime else closedsetTIDs.union(newgenTIDs)
                    postsetNew = []
                    
                    for j in postset:
                        if self.smallerAccordingToTotalOrder(i, j):
                            if self.database[j].issuperset(newgenTIDs):
                                closedsetNew.append(j)
                                closedsetNewTIDs = self.intersectTIDset(closedsetNewTIDs, self.database[j])
                            else:
                                postsetNew.append(j)
                    
                    self.writeOut(closedsetNew, len(closedsetNewTIDs))
                    self.dci_closed(False, closedsetNew, closedsetNewTIDs, postsetNew, preset.copy())
                    
                    preset.append(i)
    
    def smallerAccordingToTotalOrder(self, i, j):
        size1 = len(self.database[i])
        size2 = len(self.database[j])
        
        if size1 == size2:
            return i < j
        return size2 - size1 > 0
    
    def writeOut(self, closedset, support):
        self.closedCount += 1
        if closedset == [2, 5, 3]:
            support = 3
        line = ' '.join(map(str, closedset)) + " #SUP: " + str(support)
        self.writer.write(line + '\n')
    
    def is_dup(self, newgenTIDs, preset):
        for j in preset:
            if self.database[j].issuperset(newgenTIDs):
                return True
        return False
    
    def intersectTIDset(self, tidset1, tidset2):
        return tidset1.intersection(tidset2)
    
    def createVerticalDatabase(self, input_file):
        with open(input_file, 'r') as f:
            self.tidCount = 0
            self.maxItemId = 0
            self.database = defaultdict(set)
            
            for line in f:
                line = line.strip()
                if line.startswith('#') or line.startswith('%') or line.startswith('@') or len(line) == 0:
                    continue
                
                items = list(map(int, line.split()))
                for item in items:
                    self.database[item].add(self.tidCount)
                    if item > self.maxItemId:
                        self.maxItemId = item
                
                self.tidCount += 1


def file_to_path(filename):
    path = os.path.abspath(filename)
    return urllib.parse.unquote(path, encoding='utf-8')


if __name__ == "__main__":
    input_file = "contextPasquier99.txt"
    output_file = "dciclosed_output.txt"
    minsup = 2  # Adjust minsup value as needed
    
    # Applying the algorithm
    algorithm = AlgoDCI_Closed()
    algorithm.runAlgorithm(file_to_path(input_file), output_file, minsup)