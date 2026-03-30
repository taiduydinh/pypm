# occur.py
# Fully aligned version with Java AlgoOccur

import os
import time


# ---------------- MemoryLogger ----------------

class MemoryLogger:
    _instance = None

    def __init__(self):
        self.maxMemory = 0.0

    @staticmethod
    def getInstance():
        if MemoryLogger._instance is None:
            MemoryLogger._instance = MemoryLogger()
        return MemoryLogger._instance

    def getMaxMemory(self):
        return self.maxMemory

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self):
        return self.maxMemory


# ---------------- SequenceDatabase ----------------

class SequenceDatabase:
    def __init__(self):
        self.sequences = []
        self.itemOccurrenceCount = 0

    def loadFile(self, path):
        self.sequences = []
        self.itemOccurrenceCount = 0

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if line == "" or line[0] in ("#", "%", "@"):
                    continue

                tokens = line.split(" ")
                sequence = [int(tok) for tok in tokens]
                self.sequences.append(sequence)

    def getSequences(self):
        return self.sequences


# ---------------- AlgoOccur ----------------

class AlgoOccur:
    def __init__(self):
        self.startTime = 0
        self.endTime = 0
        self.writer = None
        self.sequenceDatabase = None

    def runAlgorithm(self, inputFile, patternFile, outputFilePath):
        self.startTime = int(time.time() * 1000)

        self.sequenceDatabase = SequenceDatabase()
        self.sequenceDatabase.loadFile(inputFile)

        self.writer = open(outputFilePath, "w", encoding="utf-8")

        self.processPatterns(patternFile)

        self.endTime = int(time.time() * 1000)
        self.writer.close()

    def processPatterns(self, patternFile):
        with open(patternFile, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")  # IMPORTANT: match Java behavior
                if line == "" or line[0] in ("#", "%", "@"):
                    continue

                posFirstSUP = line.index("#")
                sequenceText = line[:posFirstSUP - 1]

                posSID = line.index("#SID")
                sidListString = line[posSID + 6:]

                supText = line[posFirstSUP:posSID - 1]

                sids = [int(x) for x in sidListString.split()]
                pattern = [int(x) for x in sequenceText.split()]

                self.writer.write(sequenceText)
                self.writer.write(" ")
                self.writer.write(supText)
                self.writer.write(" #SIDOCC:")

                self.findOccurrences(sids, pattern)
                self.writer.write("\n")

    def findOccurrences(self, sids, pattern):
        sequences = self.sequenceDatabase.getSequences()

        for sid in sids:
            sequence = sequences[sid]
            occurrences = []

            self.findOccurrencesHelper(
                pattern, sequence, 0, 0, "", 0, occurrences
            )

            self.writer.write(" " + str(sid))
            for i, occ in enumerate(occurrences):
                self.writer.write("[")
                self.writer.write(occ)
                self.writer.write("]")
                if i != len(occurrences) - 1:
                    self.writer.write(" ")

    def findOccurrencesHelper(
        self,
        pattern,
        sequence,
        posPattern,
        posSequence,
        occurrence,
        posItemsetSequence,
        listOccurrences,
    ):
        patternResetPosition = posPattern

        while posSequence < len(sequence):

            if pattern[posPattern] == sequence[posSequence]:

                if pattern[posPattern] == -1:

                    if occurrence == "":
                        newOccurrence = str(posItemsetSequence)
                    else:
                        newOccurrence = occurrence + " " + str(posItemsetSequence)

                    if posPattern == len(pattern) - 1:
                        listOccurrences.append(newOccurrence)
                    else:
                        self.findOccurrencesHelper(
                            pattern,
                            sequence,
                            posPattern + 1,
                            posSequence + 1,
                            newOccurrence,
                            posItemsetSequence + 1,
                            listOccurrences,
                        )

                    posItemsetSequence += 1
                    posPattern = patternResetPosition

                else:
                    posPattern += 1

            elif sequence[posSequence] == -1:
                posPattern = patternResetPosition
                posItemsetSequence += 1

            posSequence += 1

    def printStatistics(self):
        print("=============  Occur 2.37 - STATISTICS =============")
        print(" Total time ~", self.endTime - self.startTime, "ms")
        print(" Max memory (mb) :", MemoryLogger.getInstance().getMaxMemory())
        print("===================================================")


# ---------------- Main ----------------

def fileToPath(filename):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, filename)


def main():
    databaseFile = fileToPath("contextPrefixSpan.txt")
    patternFile = fileToPath("spmPatterns.txt")
    outputPath = fileToPath("outputs.txt")

    algo = AlgoOccur()
    algo.runAlgorithm(databaseFile, patternFile, outputPath)
    algo.printStatistics()


if __name__ == "__main__":
    main()