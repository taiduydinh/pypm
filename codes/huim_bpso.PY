
import os
import random
import math
import time
random.seed(0)

# ================= SETTINGS =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "contextHUIM.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "output.txt")

MIN_UTILITY = 40  # change threshold here

POP_SIZE = 40
ITERATIONS = 10000
# ============================================


class Pair:
    def __init__(self, item, utility):
        self.item = item
        self.utility = utility


class Particle:
    def __init__(self, length):
        self.X = [0] * length
        self.fitness = 0

    def copy_from(self, other):
        self.X = other.X[:]
        self.fitness = other.fitness


class HUI:
    def __init__(self, itemset, fitness):
        self.itemset = itemset
        self.fitness = fitness


class AlgoHUIM_BPSO:

    def __init__(self):
        self.mapItemToTWU = {}
        self.mapItemToTWU0 = {}
        self.twuPattern = []
        self.database = []
        self.population = []
        self.pBest = []
        self.gBest = None
        self.V = []
        self.huiSets = []

    # -------------------------------------------------
    # First database scan → compute TWU
    # -------------------------------------------------
    def first_database_scan(self, input_file):
        with open(input_file) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                parts = line.split(":")
                items = list(map(int, parts[0].split()))
                transaction_utility = int(parts[1])

                for item in items:
                    self.mapItemToTWU[item] = (
                        self.mapItemToTWU.get(item, 0) + transaction_utility
                    )
                    self.mapItemToTWU0[item] = (
                        self.mapItemToTWU0.get(item, 0) + transaction_utility
                    )

    # -------------------------------------------------
    # Second database scan → build filtered database
    # -------------------------------------------------
    def second_database_scan(self, input_file, minUtility):
        with open(input_file) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue

                parts = line.split(":")
                items = list(map(int, parts[0].split()))
                utils = list(map(int, parts[2].split()))

                revisedTransaction = []

                for i in range(len(items)):
                    if self.mapItemToTWU.get(items[i], 0) >= minUtility:
                        revisedTransaction.append(Pair(items[i], utils[i]))
                    else:
                        if items[i] in self.mapItemToTWU0:
                            del self.mapItemToTWU0[items[i]]

                self.database.append(revisedTransaction)

        self.twuPattern = sorted(self.mapItemToTWU0.keys())
            # -------------------------------------------------
    # Fitness calculation (exact Java logic)
    # -------------------------------------------------
    def fitCalculate(self, particle, k):
        if k == 0:
            return 0

        fitness = 0

        for p in range(len(self.database)):
            i = 0
            j = 0
            q = 0
            temp = 0
            sum_util = 0

            while j < k and q < len(self.database[p]) and i < len(particle):
                if particle[i] == 1:
                    if self.database[p][q].item < self.twuPattern[i]:
                        q += 1
                    elif self.database[p][q].item == self.twuPattern[i]:
                        sum_util += self.database[p][q].utility
                        j += 1
                        q += 1
                        temp += 1
                        i += 1
                    else:
                        j += 1
                        i += 1
                else:
                    i += 1

            if temp == k:
                fitness += sum_util

        return fitness

    # -------------------------------------------------
    # Roulette percent (same as Java)
    # -------------------------------------------------
    def roulettePercent(self):
        percentage = []
        sum_twu = sum(self.mapItemToTWU[item] for item in self.twuPattern)

        tempSum = 0
        for item in self.twuPattern:
            tempSum += self.mapItemToTWU[item]
            percentage.append(tempSum / float(sum_twu))

        return percentage

    # -------------------------------------------------
    # Roulette selection
    # -------------------------------------------------
    def rouletteSelect(self, percentage):
        randNum = random.random()
        for i in range(len(percentage)):
            if i == 0:
                if randNum <= percentage[0]:
                    return 0
            elif percentage[i-1] < randNum <= percentage[i]:
                return i
        return len(percentage) - 1

    # -------------------------------------------------
    # Generate initial population (exact Java logic)
    # -------------------------------------------------
    def generatePop(self, minUtility):

        percentage = self.roulettePercent()

        dim = len(self.twuPattern)

        self.gBest = Particle(dim)

        for _ in range(POP_SIZE):

            particle = Particle(dim)

            j = 0
            k = int(random.random() * dim)

            while j < k:
                temp = self.rouletteSelect(percentage)
                if particle.X[temp] == 0:
                    particle.X[temp] = 1
                    j += 1

            particle.fitness = self.fitCalculate(particle.X, k)

            self.population.append(particle)

            pbest_particle = Particle(dim)
            pbest_particle.copy_from(particle)
            self.pBest.append(pbest_particle)

            if pbest_particle.fitness > self.gBest.fitness:
                self.gBest.copy_from(pbest_particle)

            # initialize velocity vector
            velocity = [random.random() for _ in range(dim)]
            self.V.append(velocity)

            # insert HUI
            if particle.fitness >= minUtility:
                self.insert(particle)
    # -------------------------------------------------
    # Update population (exact Java logic)
    # -------------------------------------------------
    def update(self, minUtility):

        dim = len(self.twuPattern)

        for i in range(POP_SIZE):

            k = 0
            r1 = random.random()
            r2 = random.random()

            # --- Update velocity ---
            for j in range(dim):

                self.V[i][j] = (
                    self.V[i][j]
                    + r1 * (self.pBest[i].X[j] - self.population[i].X[j])
                    + r2 * (self.gBest.X[j] - self.population[i].X[j])
                )

                if self.V[i][j] < -2.0:
                    self.V[i][j] = -2.0
                elif self.V[i][j] > 2.0:
                    self.V[i][j] = 2.0

            # --- Update particle bits using sigmoid ---
            for j in range(dim):

                temp1 = random.random()
                temp2 = 1.0 / (1.0 + math.exp(-self.V[i][j]))

                if temp1 < temp2:
                    self.population[i].X[j] = 1
                    k += 1
                else:
                    self.population[i].X[j] = 0

            # --- Recalculate fitness ---
            self.population[i].fitness = self.fitCalculate(
                self.population[i].X, k
            )

            # --- Update pBest ---
            if self.population[i].fitness > self.pBest[i].fitness:
                self.pBest[i].copy_from(self.population[i])

                # --- Update gBest ---
                if self.pBest[i].fitness > self.gBest.fitness:
                    self.gBest.copy_from(self.pBest[i])

            # --- Insert HUI ---
            if self.population[i].fitness >= minUtility:
                self.insert(self.population[i]) 
        # -------------------------------------------------
    # Insert HUI (exact Java logic)
    # -------------------------------------------------
    def insert(self, particle):

        temp_items = []
        for i in range(len(self.twuPattern)):
            if particle.X[i] == 1:
                temp_items.append(str(self.twuPattern[i]))

        itemset_str = " ".join(temp_items) + " "

        # Check duplicate
        for hui in self.huiSets:
            if hui.itemset == itemset_str:
                return

        self.huiSets.append(HUI(itemset_str, particle.fitness))

    # -------------------------------------------------
    # Write output
    # -------------------------------------------------
    def writeOut(self, output_file):

        with open(output_file, "w") as writer:
            for hui in self.huiSets:
                writer.write(
                    hui.itemset + "#UTIL: " + str(hui.fitness) + "\n"
                )

    # -------------------------------------------------
    # Run algorithm (exact Java structure)
    # -------------------------------------------------
    def runAlgorithm(self, input_file, output_file, minUtility):

        start_time = time.time()

        self.first_database_scan(input_file)
        self.second_database_scan(input_file, minUtility)

        if len(self.twuPattern) > 0:

            self.generatePop(minUtility)

            for _ in range(ITERATIONS):
                self.update(minUtility)

        self.writeOut(output_file)

        end_time = time.time()

        print("============= HUIM-BPSO (Python) =============")
        print("Total time ~", round((end_time - start_time) * 1000, 2), "ms")
        print("High-utility itemsets count :", len(self.huiSets))
        print("==============================================")

   # -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":

    algo = AlgoHUIM_BPSO()
    algo.runAlgorithm(INPUT_FILE, OUTPUT_FILE, MIN_UTILITY)                                