# cepb_all_in_one.py
# Single-file CEPB / CEPN / CORCEPB port (SPMF)
# Writes output to: <this_script_folder>/cepb_output.txt

from __future__ import annotations
import os
import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable
from collections import defaultdict
import time


class MemoryLogger:
    _instance = None

    def __init__(self):
        self.maxMemory = 0.0

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self):
        self.maxMemory = 0.0

    def checkMemory(self):
        return self.maxMemory
    
    def getMaxMemory(self):
        return self.maxMemory


class DataMapper:
    _key_to_val: Dict[str, int] = {}
    _val_to_key: Dict[int, str] = {}

    @staticmethod
    def mapKV(key: str) -> int:
        if key not in DataMapper._key_to_val:
            v = len(DataMapper._key_to_val)
            DataMapper._key_to_val[key] = v
            DataMapper._val_to_key[v] = key
        return DataMapper._key_to_val[key]

    @staticmethod
    def getKey(value: int) -> str:
        return DataMapper._val_to_key.get(value, "*-1*")


@dataclass
class CostUtilityPair:
    cost: float
    utility: float

    def getCost(self) -> float:
        return self.cost

    def getUtility(self) -> float:
        return self.utility


class Event:
    def __init__(self, id_: int, cost: float):
        self.id = id_
        self.cost = cost

    def getId(self) -> int:
        return self.id

    def getCost(self) -> float:
        return self.cost

    def setCost(self, cost: float):
        self.cost = cost


class EventSet:
    def __init__(self, event: Optional[int] = None):
        self.events: List[int] = []
        if event is not None:
            self.addEvent(event)

    def addEvent(self, event: int):
        self.events.append(event)

    def getEvents(self) -> List[int]:
        return self.events


class Pair:
    def __init__(self, cost: float = 0.0, totalLengthOfSeq: int = 0, indexOfNextEvent: int = 0):
        self.cost = cost
        self.totalLengthOfSeq = totalLengthOfSeq
        self.indexOfNextEvent = indexOfNextEvent

    def getTotalLengthOfSeq(self) -> int:
        return self.totalLengthOfSeq

    def getIndexOfNextEvent(self) -> int:
        return self.indexOfNextEvent

    def getCost(self) -> float:
        return self.cost


class PseudoSequence:
    def __init__(self, sequenceID: int, indexFirstItem: int, sequenceLength: int):
        self.sequenceID = sequenceID
        self.indexFirstItem = indexFirstItem
        self.sequenceLength = sequenceLength

    def getOriginalSequenceID(self) -> int:
        return self.sequenceID

    def getSequenceLength(self) -> int:
        return self.sequenceLength


class SequenceDatabase:
    def __init__(self):
        self.sequences: List[List[Event]] = []
        self.sequenceIdUtility: Dict[int, float] = {}
        self.eventOccurrenceCount = 0

    def loadFile(self, path: str):
        self.eventOccurrenceCount = 0
        self.sequences = []
        self.sequenceIdUtility = {}

        with open(path, "r", encoding="utf-8") as f:
            lineNumber = 0
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line[0] in ['#', '%', '@']:
                    continue

                tokens = line.split(" ")
                if len(tokens) < 2:
                    continue

                seq_util_tok = tokens[-1]
                pos_colon = seq_util_tok.find(":")
                if pos_colon == -1:
                    sequenceUtility = float(seq_util_tok)
                else:
                    sequenceUtility = float(seq_util_tok[pos_colon + 1 :])

                self.sequenceIdUtility[lineNumber] = sequenceUtility

                seq: List[Event] = []
                for tok in tokens[:-1]:
                    tok = tok.strip()
                    if not tok:
                        continue
                    if tok[0] != '-':
                        lb = tok.find("[")
                        rb = tok.find("]")
                        itemString = tok[:lb]
                        costString = tok[lb + 1 : rb]
                        item = DataMapper.mapKV(itemString)
                        cost = float(int(costString))
                        seq.append(Event(item, cost))
                    else:
                        seq.append(Event(int(tok), -99.0))

                self.sequences.append(seq)
                lineNumber += 1

    def size(self) -> int:
        return len(self.sequences)

    def getSequences(self) -> List[List[Event]]:
        return self.sequences

    def getSequenceUtility(self, sequenceId: int) -> float:
        return self.sequenceIdUtility[sequenceId]


class SequentialPattern:
    def __init__(self):
        self.eventsets: List[EventSet] = []
        self.sequenceIDS: List[int] = []
        self.averageCost: float = 0.0
        self.occupancy: float = 0.0
        self.correlation: float = 0.0
        self.tradeOff: float = 0.0
        self.utility: float = 0.0
        self.numInPositive: int = 0
        self.numInNegative: int = 0
        self.averageCostInPos: float = 0.0
        self.averageCostInNeg: float = 0.0
        self.costUtilityPairs: List[CostUtilityPair] = []

    def addEventset(self, eventSet: EventSet):
        self.eventsets.append(eventSet)

    def setSequencesIDs(self, seqIds: List[int]):
        self.sequenceIDS = seqIds

    def getSequencesIDs(self) -> List[int]:
        return self.sequenceIDS

    def getAbsoluteSupport(self) -> int:
        return len(self.sequenceIDS)

    def setAverageCost(self, avg: float):
        self.averageCost = avg

    def getAverageCost(self) -> float:
        return self.averageCost

    def setOccupancy(self, occ: float):
        self.occupancy = occ

    def getOccupancy(self) -> float:
        return self.occupancy

    def setUtility(self, u: float):
        self.utility = u

    def getUtility(self) -> float:
        return self.utility

    def setTradeOff(self, t: float):
        self.tradeOff = t

    def getTradeOff(self) -> float:
        return self.tradeOff

    def setCorrelation(self, c: float):
        self.correlation = c

    def getCorrelation(self) -> float:
        return self.correlation

    def setNumInNegative(self, n: int):
        self.numInNegative = n

    def setNumInPositive(self, n: int):
        self.numInPositive = n

    def setAverageCostInNeg(self, v: float):
        self.averageCostInNeg = v

    def setAverageCostInPos(self, v: float):
        self.averageCostInPos = v

    def setCostUtilityPairs(self, pairs: List[CostUtilityPair]):
        self.costUtilityPairs = pairs

    def getCostUtilityPairs(self) -> List[CostUtilityPair]:
        return self.costUtilityPairs

    def eventSetstoString(self) -> str:
        r = []
        for eventset in self.eventsets:
            for ev in eventset.getEvents():
                r.append(DataMapper.getKey(ev))
            r.append("-1")
        r.append("-2")
        return " ".join(r)


class SequentialPatterns:
    def __init__(self, name: str):
        self.levels: List[List[SequentialPattern]] = []
        self.sequenceCount = 0
        self.name = name
        self.levels.append([])

    def addSequence(self, seq: SequentialPattern, k: int):
        while len(self.levels) <= k:
            self.levels.append([])
        self.levels[k].append(seq)
        self.sequenceCount += 1


class AlgoCEPM:
    AVGCOST = " #AVGCOST: "
    TRADE = " #TRADE: "
    SUP = " #SUP: "
    UTIL = " #UTIL: "
    OCCUP = " #OCCUP: "

    class AlgorithmType:
        CEPB = "CEPB"
        CEPN = "CEPN"
        CORCEPB = "CORCEPB"

    BUFFERSSIZE = 2000
    DEBUGMODE = False

    def __init__(self):
        self.startTime = 0
        self.endTime = 0
        self.sequenceDatabase: Optional[SequenceDatabase] = None
        self.algorithmName: Optional[str] = None

        self.minimumSupport = 0
        self.maximumCost = 0.0
        self.minimumOccpuancy = 0.0

        self.patternCount = 0
        self.projectedDatabaseCount = 0
        self.consideredPatternCount = 0
        self.maximumPatternLength = 999

        self.patterns: Optional[SequentialPatterns] = None
        self.patternBuffer = [0] * self.BUFFERSSIZE

        self.sequenceIdUtility: Dict[int, float] = {}
        self.costUtilityPairs: List[CostUtilityPair] = []

        self.useLowerBound = False
        self.sortByUtilityForCEPN = False
        self.outputLowestTradeOffForCEPN = False
        self.sortByCorrelationCORCEPB = False

    def setUseLowerBound(self, useLowerBound: bool):
        self.useLowerBound = useLowerBound

    def setMaximumPatternLength(self, maximumPatternLength: int):
        self.maximumPatternLength = maximumPatternLength

    def runAlgorithmCEPB(self, inputFile: str, outputFile: str, minsup: int, maxcost: float, minoccupancy: float):
        self.algorithmName = self.AlgorithmType.CEPB
        return self.runAlgorithm(inputFile, outputFile, minsup, maxcost, minoccupancy)

    def runAlgorithm(self, inputFile: str, outputFile: Optional[str], minsup: int, maxcost: float, minoccupancy: float):
        self.minimumSupport = minsup
        self.maximumCost = maxcost
        self.minimumOccpuancy = minoccupancy

        MemoryLogger.getInstance().reset()
        self.startTime = int(time.time() * 1000)

        self.sequenceDatabase = SequenceDatabase()
        self.sequenceDatabase.loadFile(inputFile)
        self.sequenceIdUtility = self.sequenceDatabase.sequenceIdUtility

        self.patterns = SequentialPatterns("SEQUENTIAL LOWER BOUND PATTERN MINING")
        mapSequenceID = self.findSequencesContainingItems()
        self.prefixSpanWithSingleItem(mapSequenceID)
        self.sequenceDatabase = None

        if outputFile is not None:
            if self.algorithmName == self.AlgorithmType.CEPB:
                self.writeResultsToFileCEPB(outputFile)
            else:
                self.writeResultsToFileCEPB(outputFile)

        self.endTime = int(time.time() * 1000)
        return self.patterns

    def writeResultsToFileCEPB(self, outputFile: str):
        def fmt(x: float) -> str:
            return f"{x:.3f}"
        with open(outputFile, "w", encoding="utf-8") as w:
            for level in self.patterns.levels:
                for pat in level:
                    w.write(pat.eventSetstoString())
                    w.write(self.SUP + str(pat.getAbsoluteSupport()))
                    w.write(self.AVGCOST + fmt(pat.getAverageCost()))
                    w.write(self.OCCUP + fmt(pat.getOccupancy()))
                    w.write("\n")

    def printStatistics(self):
        r = []
        r.append(f"=============  {self.algorithmName} 2.42 STATISTICS =============")
        r.append(f" Pattern count : {self.patternCount}")
        r.append(f" Total time : {self.endTime - self.startTime} ms")
        r.append(f" Max memory (mb) : {MemoryLogger.getInstance().getMaxMemory()}")
        if self.DEBUGMODE:
            r.append(f"  Projected Database Count: {self.projectedDatabaseCount}")
            r.append(f"  Considered pattern count: {self.consideredPatternCount}")
            r.append(f" Frequent sequences count : {self.patterns.sequenceCount}")
        r.append("===================================================")
        print("\n".join(r))

    # ---------- core mining (Java-faithful) ----------

    def findSequencesContainingItems(self) -> Dict[int, Dict[int, Pair]]:
        m: Dict[int, Dict[int, Pair]] = {}
        for i in range(self.sequenceDatabase.size()):
            seq = self.sequenceDatabase.getSequences()[i]
            for token in seq:
                if token.getId() >= 0:
                    if token.getId() not in m:
                        m[token.getId()] = {i: Pair(token.getCost(), len(seq), i + 1)}
                    else:
                        if i not in m[token.getId()]:
                            m[token.getId()][i] = Pair(token.getCost(), len(seq), i + 1)
        return m

    def prefixSpanWithSingleItem(self, mapSequenceID: Dict[int, Dict[int, Pair]]):
        # remove infrequent events + resize sequences after removal (Java logic)
        for i in range(self.sequenceDatabase.size()):
            seq = self.sequenceDatabase.getSequences()[i]
            currentPosition = 0
            j = 0
            while j < len(seq):
                token = seq[j]
                if token.getId() >= 0:
                    isFrequent = len(mapSequenceID.get(token.getId(), {})) >= self.minimumSupport
                    if isFrequent:
                        seq[currentPosition] = token
                        currentPosition += 1
                elif token.getId() == -2:
                    if currentPosition > 0:
                        seq[currentPosition] = Event(-2, -99)
                    newSeq = seq[: currentPosition + 1]
                    self.sequenceDatabase.getSequences()[i] = newSeq
                    break
                j += 1

        # CEPB/CEPN case only (your current run is CEPB)
        if self.algorithmName in (self.AlgorithmType.CEPB, self.AlgorithmType.CEPN):
            for event, seqMap in mapSequenceID.items():
                self.consideredPatternCount += 1
                support = len(seqMap)
                sequenceIDs = list(seqMap.keys())

                if support >= self.minimumSupport:
                    avgCost = self.getAverageCostWithSingleEvent(seqMap)
                    occupancy = self.getOccupancyWithSingleEvent(seqMap)

                    if avgCost <= self.maximumCost and occupancy >= self.minimumOccpuancy:
                        self.costUtilityPairs = self.getListOfCostUtility(seqMap)
                        self.savePattern_single(event, avgCost, occupancy, seqMap, self.costUtilityPairs)

                    lowerSupportCost = self.getLowerBound_single(self.minimumSupport, seqMap)
                    lowerBoundOfCost = lowerSupportCost / self.minimumSupport
                    upperBoundOfOccupancy = self.getUpperBoundOccupancyWithSingleEvnet(seqMap)

                    if ((lowerBoundOfCost <= self.maximumCost and upperBoundOfOccupancy >= self.minimumOccpuancy)
                        or (self.useLowerBound is False)):

                        self.patternBuffer[0] = event
                        if self.maximumPatternLength > 1:
                            projectedDB = self.buildProjectedDatabaseSingleItems(event, sequenceIDs)
                            self.projectedDatabaseCount += 1
                            self.recursionSingleEvents(projectedDB, 2, 0)

    # ----- single-event metrics -----

    def getListOfCostUtility(self, seqIdPair: Dict[int, Pair]) -> List[CostUtilityPair]:
        out: List[CostUtilityPair] = []
        for sid, p in seqIdPair.items():
            out.append(CostUtilityPair(p.getCost(), self.sequenceIdUtility[sid]))
        return out

    def getAverageCostWithSingleEvent(self, sequenceIdCost: Dict[int, Pair]) -> float:
        s = 0.0
        for _, p in sequenceIdCost.items():
            s += p.getCost()
        return s / len(sequenceIdCost)

    def getLowerBound_single(self, minimumSupport: int, sequenceIdCost: Dict[int, Pair]) -> float:
        costs = [p.getCost() for p in sequenceIdCost.values()]
        costs.sort()
        return sum(costs[:minimumSupport])

    def getOccupancyWithSingleEvent(self, sequenceIDLength: Dict[int, Pair]) -> float:
        occ = 0.0
        for _, p in sequenceIDLength.items():
            lenthOfSeq = p.getTotalLengthOfSeq() - 1
            occ += (1.0 / lenthOfSeq)
        return occ / len(sequenceIDLength)

    def getUpperBoundOccupancyWithSingleEvnet(self, sequenceIDLength: Dict[int, Pair]) -> float:
        upperOccupList: List[float] = []
        for _, p in sequenceIDLength.items():
            lenOfSeq = p.getTotalLengthOfSeq() - 1
            upperOccupList.append((1.0 + (lenOfSeq - p.getIndexOfNextEvent())) / lenOfSeq)
        upperOccupList.sort(reverse=True)
        return sum(upperOccupList[: self.minimumSupport]) / self.minimumSupport

    # ----- save single-event pattern -----

    def savePattern_single(self, event: int, avgCost: float, occupancy: float,
                           seqIdCost: Dict[int, Pair], pairs: List[CostUtilityPair]):
        self.patternCount += 1
        pat = SequentialPattern()
        pat.addEventset(EventSet(event))
        pat.setSequencesIDs(list(seqIdCost.keys()))
        pat.setAverageCost(avgCost)
        pat.setOccupancy(occupancy)
        pat.setCostUtilityPairs(pairs)
        self.patterns.addSequence(pat, 1)

    # ----- projection / recursion (2-event and beyond) -----

    def buildProjectedDatabaseSingleItems(self, event: int, sequenceIDs: List[int]) -> List[PseudoSequence]:
        projected: List[PseudoSequence] = []
        for sequenceID in sequenceIDs:
            seq = self.sequenceDatabase.getSequences()[sequenceID]
            j = 0
            while j < len(seq) and seq[j].getId() != -2:
                token = seq[j].getId()
                if token == event:
                    if j + 1 < len(seq) and seq[j + 1].getId() != -2:
                        projected.append(PseudoSequence(sequenceID, j + 1, len(seq)))
                    break
                j += 1
            MemoryLogger.getInstance().checkMemory()
        return projected

    def findAllFrequentPairsSingleEvents(self, sequences: List[PseudoSequence]) -> Dict[int, List[PseudoSequence]]:
        m: Dict[int, List[PseudoSequence]] = {}
        for ps in sequences:
            sequenceID = ps.getOriginalSequenceID()
            seq = self.sequenceDatabase.getSequences()[sequenceID]
            i = ps.indexFirstItem
            while i < len(seq) and seq[i].getId() != -2:
                token = seq[i].getId()
                if token >= 0:
                    if token not in m:
                        m[token] = []
                    lst = m[token]
                    ok = True
                    if len(lst) > 0:
                        ok = (lst[-1].sequenceID != sequenceID)
                    if ok:
                        lst.append(PseudoSequence(sequenceID, i + 1, len(seq)))
                MemoryLogger.getInstance().checkMemory()
                i += 1
        return m

    def recursionSingleEvents(self, database: List[PseudoSequence], k: int, lastBufferPositionOfPattern: int):
        eventsPseudoSequence = self.findAllFrequentPairsSingleEvents(database)
        database = None

        if self.algorithmName in (self.AlgorithmType.CEPB, self.AlgorithmType.CEPN):
            for event, pslist in eventsPseudoSequence.items():
                self.consideredPatternCount += 1
                support = len(pslist)
                if support >= self.minimumSupport:
                    self.patternBuffer[lastBufferPositionOfPattern + 1] = -1
                    self.patternBuffer[lastBufferPositionOfPattern + 2] = event

                    lowerSupportCost = self.getLowerBound_multi(lastBufferPositionOfPattern, event, pslist)
                    lowerBoundOfCost = lowerSupportCost / self.minimumSupport

                    avgCost = self.getAverageCostWithMulEvents(lastBufferPositionOfPattern, pslist, event)
                    occupancy = self.getOccupancyWithMultipleEvents(pslist, k)

                    # Java code (as provided) uses getOccupancyWithMultipleEvents for "upperBound" too in this branch
                    upperBoundOfOccupancy = self.getOccupancyWithMultipleEvents(pslist, k)

                    if avgCost <= self.maximumCost and occupancy >= self.minimumOccpuancy:
                        self.costUtilityPairs = self.setListOfCostUtility(lastBufferPositionOfPattern + 2, pslist)
                        self.savePattern_multi(lastBufferPositionOfPattern + 2, pslist, avgCost, occupancy, self.costUtilityPairs)

                    if ((lowerBoundOfCost <= self.maximumCost and upperBoundOfOccupancy >= self.minimumOccpuancy)
                        or (not self.useLowerBound)):
                        if k < self.maximumPatternLength:
                            self.projectedDatabaseCount += 1
                            self.recursionSingleEvents(pslist, k + 1, lastBufferPositionOfPattern + 2)

                MemoryLogger.getInstance().checkMemory()

    def getOccupancyWithMultipleEvents(self, pseudoSequenceList: List[PseudoSequence], patternLength: float) -> float:
        occ = 0.0
        for ps in pseudoSequenceList:
            lengthOfSeq = ps.getSequenceLength() - 1
            occ += patternLength / lengthOfSeq
        return occ / len(pseudoSequenceList)

    def getLowerBound_multi(self, lastBufferPosition: int, currentEvent: int, pseudoSequences: List[PseudoSequence]) -> float:
        eventsBeforeCurrent: List[int] = []
        for i in range(0, lastBufferPosition + 1):
            token = self.patternBuffer[i]
            if token >= 0:
                eventsBeforeCurrent.append(token)
        eventsBeforeCurrent.append(currentEvent)

        costs: List[float] = []
        for ps in pseudoSequences:
            costOfPattern = 0.0
            currentEventPosition = ps.indexFirstItem - 1
            sequenceId = ps.sequenceID
            events = self.sequenceDatabase.getSequences()[sequenceId]
            j = 0
            i = 0
            while i <= currentEventPosition and j < len(eventsBeforeCurrent):
                if events[i].getId() == eventsBeforeCurrent[j]:
                    costOfPattern += events[i].getCost()
                    j += 1
                i += 1
            costs.append(costOfPattern)

        costs.sort()
        return sum(costs[: self.minimumSupport])

    def getAverageCostWithMulEvents(self, lastBufferPosition: int, pseudoSequences: List[PseudoSequence], currentEvent: int) -> float:
        eventsBeforeCurrent: List[int] = []
        for i in range(0, lastBufferPosition + 1):
            token = self.patternBuffer[i]
            if token >= 0:
                eventsBeforeCurrent.append(token)
        eventsBeforeCurrent.append(currentEvent)

        totalCost = 0.0
        for ps in pseudoSequences:
            currentEventPosition = ps.indexFirstItem - 1
            sequenceId = ps.sequenceID
            events = self.sequenceDatabase.getSequences()[sequenceId]
            j = 0
            i = 0
            while i <= currentEventPosition and j < len(eventsBeforeCurrent):
                if events[i].getId() == eventsBeforeCurrent[j]:
                    totalCost += events[i].getCost()
                    j += 1
                i += 1
        return totalCost / len(pseudoSequences)

    def setListOfCostUtility(self, lastBufferPosition: int, pseudoSequences: List[PseudoSequence]) -> List[CostUtilityPair]:
        eventsIdBeforeCurrent: List[int] = []
        currentEventset = EventSet()

        for i in range(0, lastBufferPosition + 1):
            token = self.patternBuffer[i]
            if token >= 0:
                currentEventset.addEvent(token)
                eventsIdBeforeCurrent.append(token)
            elif token == -1:
                currentEventset = EventSet()

        out: List[CostUtilityPair] = []
        for ps in pseudoSequences:
            currentEventPosition = ps.indexFirstItem - 1
            sequenceId = ps.sequenceID
            costOfPattern = 0.0
            events = self.sequenceDatabase.getSequences()[sequenceId]
            j = 0
            i = 0
            while i <= currentEventPosition and j < len(eventsIdBeforeCurrent):
                if events[i].getId() == eventsIdBeforeCurrent[j]:
                    costOfPattern += events[i].getCost()
                    j += 1
                i += 1
            out.append(CostUtilityPair(costOfPattern, self.sequenceIdUtility[sequenceId]))
        return out

    def savePattern_multi(self, lastBufferPosition: int, pseudoSequences: List[PseudoSequence],
                          avgCost: float, occupancy: float, pairs: List[CostUtilityPair]):
        self.patternCount += 1
        eventsIdBeforeCurrent: List[int] = []

        pat = SequentialPattern()
        eventsetCount = 0
        currentEventset = EventSet()

        for i in range(0, lastBufferPosition + 1):
            token = self.patternBuffer[i]
            if token >= 0:
                currentEventset.addEvent(token)
                eventsIdBeforeCurrent.append(token)
            elif token == -1:
                pat.addEventset(currentEventset)
                currentEventset = EventSet()
                eventsetCount += 1

        pat.addEventset(currentEventset)
        eventsetCount += 1

        seqIds = [ps.sequenceID for ps in pseudoSequences]
        pat.setSequencesIDs(seqIds)
        pat.setAverageCost(avgCost)
        pat.setOccupancy(occupancy)
        pat.setCostUtilityPairs(pairs)

        # CEPN tradeOff/utility not used for CEPB run; keep structure minimal
        self.patterns.addSequence(pat, eventsetCount)


def _resolve_path_near_script(filename: str) -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    p1 = filename
    if os.path.isabs(p1) and os.path.exists(p1):
        return p1
    p2 = os.path.join(here, filename)
    if os.path.exists(p2):
        return p2
    return p2


if __name__ == "__main__":
    # Default: mimic MainTestCEPB settings.
    # Put your input file (e.g., example_CEPB.txt) in the SAME folder as this script, or pass a full path.
    input_name = "example_CEPB.txt"
    input_path = _resolve_path_near_script(input_name)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cepb_output.txt")

    algo = AlgoCEPM()
    algo.setUseLowerBound(True)

    minsup = 2
    maxcost = 50.0
    minoccupancy = 0.1

    algo.runAlgorithmCEPB(input_path, out_path, minsup, maxcost, minoccupancy)
    algo.printStatistics()
