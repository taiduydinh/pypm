#!/usr/bin/env python3
"""
Python implementation of the SPADE algorithm for sequential pattern mining, based on the Java implementation from the SPMF library.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Dict, Iterable, List, Optional, Set, Tuple


class MemoryLogger:
    """Lightweight memory logger to mimic the Java helper."""

    _instance: ClassVar[Optional["MemoryLogger"]] = None

    def __init__(self) -> None:
        self._max_memory = 0.0

    @classmethod
    def get_instance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def reset(self) -> None:
        self._max_memory = 0.0

    def check_memory(self) -> float:
        # Python stdlib has no direct equivalent of the Java call; keep a stub.
        return self._max_memory

    def get_max_memory(self) -> float:
        return self._max_memory


class AbstractionGeneric:
    def compare_to(self, other: "AbstractionGeneric") -> int:
        raise NotImplementedError

    def to_file_str(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class AbstractionQualitative(AbstractionGeneric):
    equal_relation: bool
    _pool: ClassVar[Dict[bool, "AbstractionQualitative"]] = {}

    @classmethod
    def create(cls, equal_relation: bool) -> "AbstractionQualitative":
        if not cls._pool:
            cls._pool[True] = cls(True)
            cls._pool[False] = cls(False)
        return cls._pool[equal_relation]

    def compare_to(self, other: AbstractionGeneric) -> int:
        if not isinstance(other, AbstractionQualitative):
            return -1
        if self.equal_relation == other.equal_relation:
            return 0
        return -1 if not self.equal_relation else 1

    def __lt__(self, other: "AbstractionQualitative") -> bool:
        return self.compare_to(other) < 0

    def __str__(self) -> str:
        return "" if self.equal_relation else " ->"

    def to_file_str(self) -> str:
        return "" if self.equal_relation else " -1"


@dataclass(frozen=True)
class Item:
    id: int

    def __lt__(self, other: "Item") -> bool:
        # Descending order to mimic original Java negative compareTo used across the ports.
        return self.id > other.id

    def __str__(self) -> str:
        return str(self.id)


@dataclass(frozen=True)
class ItemAbstractionPair:
    item: Item
    abstraction: AbstractionGeneric

    def __lt__(self, other: "ItemAbstractionPair") -> bool:
        if self.item != other.item:
            return self.item < other.item
        if self.abstraction == other.abstraction:
            return False
        return self.abstraction.compare_to(other.abstraction) < 0

    def __str__(self) -> str:
        if isinstance(self.abstraction, AbstractionQualitative):
            return f"{self.abstraction}{self.item}"
        return f"{self.item}{self.abstraction}"

    def to_file_tokens(self) -> List[str]:
        if isinstance(self.abstraction, AbstractionQualitative):
            if not self.abstraction.equal_relation:
                return ["-1", str(self.item)]
            return [str(self.item)]
        return [f"{self.item}{self.abstraction}"]

class Pattern:
    def __init__(self, elements: Optional[Iterable[ItemAbstractionPair]] = None) -> None:
        self.elements: List[ItemAbstractionPair] = list(elements) if elements else []
        self.appearing_in: Set[int] = set()

    def __len__(self) -> int:
        return len(self.elements)

    def __iter__(self):
        return iter(self.elements)

    def __lt__(self, other: "Pattern") -> bool:
        # Lexicographic compare on elements then length.
        for a, b in zip(self.elements, other.elements):
            if a == b:
                continue
            return a < b
        return len(self.elements) < len(other.elements)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Pattern):
            return False
        return self.elements == other.elements

    def __hash__(self) -> int:
        return hash(tuple(self.elements))

    def __str__(self) -> str:
        result = "".join(str(el) for el in self.elements)
        ids = ", ".join(str(i) for i in sorted(self.appearing_in))
        return f"{result}\t[{ids}]"

    def to_string_to_file(self, output_sequence_identifiers: bool = False) -> str:
        tokens: List[str] = []
        if not self.elements:
            return ""
        for idx, pair in enumerate(self.elements):
            if idx == len(self.elements) - 1:
                if idx != 0:
                    tokens.extend(pair.to_file_tokens())
                else:
                    tokens.append(str(pair.item))
                tokens.append("-1")
            elif idx == 0:
                tokens.append(str(pair.item))
            else:
                tokens.extend(pair.to_file_tokens())
        tokens.append(f"#SUP: {len(self.appearing_in)}")
        if output_sequence_identifiers and self.appearing_in:
            tokens.append("#SID:")
            tokens.extend(str(i) for i in sorted(self.appearing_in))
        return " ".join(tokens)

    def clone_pattern(self) -> "Pattern":
        # Keep only the structure of the pattern; support will be recomputed.
        return Pattern(list(self.elements))

    def get_elements(self) -> List[ItemAbstractionPair]:
        return self.elements

    def get_ith_element(self, i: int) -> ItemAbstractionPair:
        return self.elements[i]

    def get_last_but_one_element(self) -> Optional[ItemAbstractionPair]:
        return self.elements[-2] if len(self.elements) > 1 else None

    def get_last_element(self) -> Optional[ItemAbstractionPair]:
        return self.elements[-1] if self.elements else None

    def add(self, pair: ItemAbstractionPair) -> None:
        self.elements.append(pair)

    def size(self) -> int:
        return len(self.elements)

    def is_prefix(self, other: "Pattern") -> bool:
        return len(self.elements) > 0 and self.elements[:-1] == other.elements[:-1]

    def add_appearance(self, sequence_id: int) -> None:
        self.appearing_in.add(sequence_id)

    def get_support(self) -> int:
        return len(self.appearing_in)


class Itemset:
    def __init__(self, items: Optional[Iterable[Item]] = None, timestamp: int = 0) -> None:
        self.items: List[Item] = list(items) if items else []
        self.timestamp: int = timestamp

    def add_item(self, value: Item) -> None:
        self.items.append(value)

    def add_item_at(self, index: int, value: Item) -> None:
        if index < len(self.items):
            self.items[index] = value
        else:
            self.items.append(value)

    def remove_item(self, index: int) -> Item:
        return self.items.pop(index)

    def remove_item_value(self, item: Item) -> bool:
        try:
            self.items.remove(item)
            return True
        except ValueError:
            return False

    def index_of(self, item: Item, start: int = 0) -> Optional[int]:
        for idx in range(start, len(self.items)):
            if self.items[idx] == item:
                return idx
        return None

    def clone_itemset(self) -> "Itemset":
        clone = Itemset(list(self.items), self.timestamp)
        return clone

    def size(self) -> int:
        return len(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index: int) -> Item:
        return self.items[index]

    def __str__(self) -> str:
        return " ".join(str(item) for item in self.items)


class Sequence:
    def __init__(self, sequence_id: int) -> None:
        self.id = sequence_id
        self.itemsets: List[Itemset] = []
        self.number_of_items = 0

    def add_itemset(self, itemset: Itemset) -> None:
        self.itemsets.append(itemset)
        self.number_of_items += itemset.size()

    def add_item(self, item: Item) -> None:
        if not self.itemsets:
            self.add_itemset(Itemset())
        self.itemsets[-1].add_item(item)
        self.number_of_items += 1

    def add_item_in_itemset(self, index_itemset: int, item: Item) -> None:
        self.itemsets[index_itemset].add_item(item)
        self.number_of_items += 1

    def add_item_at_position(self, index_itemset: int, index_item: int, item: Item) -> None:
        self.itemsets[index_itemset].add_item_at(index_item, item)
        self.number_of_items += 1

    def remove_itemset(self, index_itemset: int) -> Itemset:
        itemset = self.itemsets.pop(index_itemset)
        self.number_of_items -= itemset.size()
        return itemset

    def remove_item(self, index_itemset: int, index_item: int) -> Item:
        self.number_of_items -= 1
        return self.itemsets[index_itemset].remove_item(index_item)

    def remove_item_value(self, index_itemset: int, item: Item) -> None:
        removed = self.itemsets[index_itemset].remove_item_value(item)
        if removed:
            self.number_of_items -= 1

    def clone_sequence(self) -> "Sequence":
        seq = Sequence(self.id)
        for itemset in self.itemsets:
            seq.add_itemset(itemset.clone_itemset())
        return seq

    def __str__(self) -> str:
        parts = []
        for itemset in self.itemsets:
            items_str = " ".join(str(item) for item in itemset.items)
            parts.append(f"{{t={itemset.timestamp}, {items_str}}}")
        return "".join(parts) + "    "

    def __len__(self) -> int:
        return len(self.itemsets)

    def __getitem__(self, index: int) -> Itemset:
        return self.itemsets[index]

    def size(self) -> int:
        return len(self.itemsets)

    def get_length(self) -> int:
        return self.number_of_items

    def get_time_length(self) -> int:
        if not self.itemsets:
            return 0
        return self.itemsets[-1].timestamp - self.itemsets[0].timestamp

    def search_for_first_appearance(self, itemset_index: int, item_index: int, item: Item) -> Optional[Tuple[int, int]]:
        if itemset_index >= len(self.itemsets):
            return None
        for i in range(itemset_index, len(self.itemsets)):
            current = self.itemsets[i]
            start = item_index if i == itemset_index else 0
            pos = current.index_of(item, start)
            if pos is not None:
                return i, pos
        return None

    def search_for_item_at_same_itemset(self, item: Item, itemset_index: int, item_index: int) -> Optional[Tuple[int, int]]:
        if itemset_index >= len(self.itemsets):
            return None
        current = self.itemsets[itemset_index]
        pos = current.index_of(item, item_index)
        if pos is not None and pos >= item_index:
            return itemset_index, pos
        return None

    def search_for_item_in_concrete_temporal_distance(self, item: Item, itemset_index: int, item_index: int, temporal_distance: int) -> Optional[Tuple[int, int]]:
        if itemset_index >= len(self.itemsets):
            return None
        initial_timestamp = self.itemsets[itemset_index].timestamp
        objective_timestamp = initial_timestamp + temporal_distance
        itemset = itemset_index + 1
        while itemset < len(self.itemsets) and self.itemsets[itemset].timestamp < objective_timestamp:
            itemset += 1
        if itemset < len(self.itemsets) and self.itemsets[itemset].timestamp == objective_timestamp:
            current = self.itemsets[itemset]
            pos = current.index_of(item, 0)
            if pos is not None:
                return itemset, pos
        return None

    def number_of_items_after_position(self, itemset_index: int, item_index: int) -> int:
        size = 0
        if itemset_index < len(self.itemsets) - 1:
            for idx in range(itemset_index + 1, len(self.itemsets)):
                size += len(self.itemsets[idx])
        size += len(self.itemsets[itemset_index]) - item_index - 1
        return size

class ItemFactory:
    def __init__(self) -> None:
        self.pool: Dict[int, Item] = {}

    def get_item(self, key: int) -> Item:
        item = self.pool.get(key)
        if item is None:
            item = Item(key)
            self.pool[key] = item
        return item


class PatternCreator:
    _instance: ClassVar[Optional["PatternCreator"]] = None

    def __init__(self) -> None:
        pass

    @classmethod
    def get_instance(cls) -> "PatternCreator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def create_pattern(self, elements: Optional[Iterable[ItemAbstractionPair]] = None) -> Pattern:
        return Pattern(elements)


class ItemAbstractionPairCreator:
    _instance: ClassVar[Optional["ItemAbstractionPairCreator"]] = None

    def __init__(self) -> None:
        pass

    @classmethod
    def get_instance(cls) -> "ItemAbstractionPairCreator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_item_abstraction_pair(self, item: Item, abstraction: AbstractionGeneric) -> ItemAbstractionPair:
        return ItemAbstractionPair(item, abstraction)


class AbstractionCreator:
    def create_default_abstraction(self) -> AbstractionGeneric:
        raise NotImplementedError

    def create_size2_sequences(self, sequences: List[Sequence]) -> List[Pattern]:
        raise NotImplementedError

    def get_subpattern(self, extension: Pattern, index: int) -> Pattern:
        raise NotImplementedError

    def create_size2_sequences_from_map(self, bbdd_horizontal: Dict[int, Dict[Item, List[int]]], frequent_items: Dict[Item, Pattern]) -> List[Pattern]:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError

    def create_abstraction(self, current_time: int, previous_time: int) -> AbstractionGeneric:
        raise NotImplementedError

    def find_position_of_item_in_sequence(
        self,
        sequence: Sequence,
        item_pair: Item,
        abs_pair: AbstractionGeneric,
        abs_previous: Optional[AbstractionGeneric],
        itemset_index: int,
        item_index: int,
        previous_itemset_index: int,
        previous_item_index: int,
    ) -> Optional[Tuple[int, int]]:
        raise NotImplementedError

    def generate_candidates(self, creator: "AbstractionCreator", pattern1: Pattern, pattern2: Pattern, min_support: float) -> List[Pattern]:
        raise NotImplementedError

    def is_candidate_in_sequence(self, finder: "CandidateInSequenceFinder", candidate: Pattern, sequence: Sequence, k: int, i: int, position: List[List[int]]) -> None:
        raise NotImplementedError

    def generate_size2_candidates(self, creator: "AbstractionCreator", pat1: Pattern, pat2: Pattern) -> List[Pattern]:
        raise NotImplementedError


class AbstractionCreatorQualitative(AbstractionCreator):
    _instance: ClassVar[Optional["AbstractionCreatorQualitative"]] = None

    def __init__(self) -> None:
        pass

    @classmethod
    def get_instance(cls) -> "AbstractionCreatorQualitative":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def create_default_abstraction(self) -> AbstractionGeneric:
        return AbstractionQualitative.create(False)

    def create_abstraction_flag(self, appearing_in_same_itemset: bool) -> AbstractionGeneric:
        return AbstractionQualitative.create(appearing_in_same_itemset)

    def create_size2_sequences(self, sequences: List[Sequence]) -> List[Pattern]:
        total_map: Dict[Pattern, Pattern] = {}
        output: List[Pattern] = []
        pattern_creator = PatternCreator.get_instance()
        pair_creator = ItemAbstractionPairCreator.get_instance()
        for sequence in sequences:
            itemsets = sequence.itemsets
            for i, current_itemset in enumerate(itemsets):
                for j in range(len(current_itemset)):
                    item1 = current_itemset[j]
                    pair1 = ItemAbstractionPair(item1, self.create_default_abstraction())
                    for k in range(j + 1, len(current_itemset)):
                        item2 = current_itemset[k]
                        pair2 = ItemAbstractionPair(item2, AbstractionQualitative.create(True))
                        self._update_appearance_set(total_map, pair1, pair2, sequence.id, pattern_creator)
                    for next_idx in range(i + 1, len(itemsets)):
                        next_itemset = itemsets[next_idx]
                        for item2 in next_itemset:
                            pair2 = ItemAbstractionPair(item2, AbstractionQualitative.create(False))
                            self._update_appearance_set(total_map, pair1, pair2, sequence.id, pattern_creator)
        output.extend(total_map.values())
        output.sort()
        return output

    def _update_appearance_set(
        self,
        total_map: Dict[Pattern, Pattern],
        pair1: ItemAbstractionPair,
        pair2: ItemAbstractionPair,
        seq_id: int,
        pattern_creator: PatternCreator,
    ) -> None:
        elements = [pair1, pair2]
        new_pattern = pattern_creator.create_pattern(elements)
        existing_pattern = total_map.get(new_pattern)
        if existing_pattern is None:
            existing_pattern = new_pattern
            total_map[new_pattern] = new_pattern
        existing_pattern.add_appearance(seq_id)

    def get_subpattern(self, extension: Pattern, index: int) -> Pattern:
        pair_creator = ItemAbstractionPairCreator.get_instance()
        pattern_creator = PatternCreator.get_instance()
        subpattern_elements: List[ItemAbstractionPair] = []
        abstraction: Optional[AbstractionGeneric] = None
        next_index = index + 1
        for i in range(extension.size()):
            if i != index:
                if i == next_index:
                    if abstraction is None:
                        abstraction = extension.get_ith_element(i).abstraction
                    subpattern_elements.append(pair_creator.get_item_abstraction_pair(extension.get_ith_element(i).item, abstraction))
                else:
                    subpattern_elements.append(extension.get_ith_element(i))
            else:
                if index == 0:
                    abstraction = self.create_default_abstraction()
                else:
                    removed_abs = extension.get_ith_element(i).abstraction
                    if isinstance(removed_abs, AbstractionQualitative) and not removed_abs.equal_relation:
                        abstraction = self.create_abstraction_flag(False)
        return pattern_creator.create_pattern(subpattern_elements)

    def create_size2_sequences_from_map(self, bbdd: Dict[int, Dict[Item, List[int]]], frequent_items: Dict[Item, Pattern]) -> List[Pattern]:
        total_map: Dict[Pattern, Pattern] = {}
        output: List[Pattern] = []
        pattern_creator = PatternCreator.get_instance()
        for seq_id, entries in bbdd.items():
            item_itemset_associations = list(entries.items())
            for i, (item1, appearances1) in enumerate(item_itemset_associations):
                if not self._is_frequent(item1, frequent_items):
                    continue
                for appearance1 in appearances1:
                    pair1 = ItemAbstractionPair(item1, self.create_default_abstraction())
                    for j, (item2, appearances2) in enumerate(item_itemset_associations):
                        if not self._is_frequent(item2, frequent_items):
                            continue
                        for appearance2 in appearances2:
                            pair2: Optional[ItemAbstractionPair] = None
                            if appearance2 == appearance1:
                                if -1 * self._item_compare(item2, item1) == 1:
                                    pair2 = ItemAbstractionPair(item2, AbstractionQualitative.create(True))
                            elif appearance2 > appearance1:
                                pair2 = ItemAbstractionPair(item2, AbstractionQualitative.create(False))
                            if pair2 is not None:
                                elements = [pair1, pair2]
                                new_pattern = pattern_creator.create_pattern(elements)
                                existing_pattern = total_map.get(new_pattern)
                                if existing_pattern is None:
                                    existing_pattern = new_pattern
                                    total_map[new_pattern] = new_pattern
                                existing_pattern.add_appearance(seq_id)
        output.extend(total_map.values())
        output.sort()
        return output

    def _is_frequent(self, item: Item, frequent_items: Dict[Item, Pattern]) -> bool:
        return frequent_items.get(item) is not None

    def clear(self) -> None:
        AbstractionQualitative._pool.clear()
        AbstractionCreatorQualitative._instance = None

    def create_abstraction(self, current_time: int, previous_time: int) -> AbstractionGeneric:
        in_same_itemset = current_time == previous_time
        return AbstractionQualitative.create(in_same_itemset)

    def find_position_of_item_in_sequence(
        self,
        sequence: Sequence,
        item_pair: Item,
        abs_pair: AbstractionGeneric,
        previous_abs: Optional[AbstractionGeneric],
        itemset_index: int,
        item_index: int,
        previous_itemset_index: int,
        previous_item_index: int,
    ) -> Optional[Tuple[int, int]]:
        abs_qual = abs_pair if isinstance(abs_pair, AbstractionQualitative) else AbstractionQualitative.create(False)
        if abs_qual.equal_relation:
            if itemset_index == previous_itemset_index:
                return sequence.search_for_item_at_same_itemset(item_pair, itemset_index, item_index)
            return None
        itemset_index_to_search = itemset_index
        item_index_to_search = item_index
        if itemset_index == previous_itemset_index:
            itemset_index_to_search += 1
            item_index_to_search = 0
        return sequence.search_for_first_appearance(itemset_index_to_search, item_index_to_search, item_pair)

    def generate_candidates(self, creator: "AbstractionCreator", pattern1: Pattern, pattern2: Pattern, min_support: float) -> List[Pattern]:
        """Generate all possible pattern extensions (up to three) as in the Java SPADE candidate generator."""
        candidates: List[Pattern] = []
        # Prefix check: patterns must share all elements except their respective last one
        elements1 = pattern1.get_elements()
        elements2 = pattern2.get_elements()
        prefix_length = len(elements1) - 1
        for i in range(prefix_length):
            if elements1[i] != elements2[i]:
                return candidates

        # Quick support upper-bound: intersection of appearing sequences
        if len(pattern1.appearing_in & pattern2.appearing_in) < min_support:
            return candidates

        pair_creator = ItemAbstractionPairCreator.get_instance()
        qual_creator = AbstractionCreatorQualitative.get_instance()
        last_pair_1 = pattern1.get_last_element()
        last_pair_2 = pattern2.get_last_element()
        if last_pair_1 is None or last_pair_2 is None:
            return candidates
        abs1 = last_pair_1.abstraction
        abs2 = last_pair_2.abstraction
        eq1 = isinstance(abs1, AbstractionQualitative) and abs1.equal_relation
        eq2 = isinstance(abs2, AbstractionQualitative) and abs2.equal_relation

        if not eq1 and not eq2:
            # Different itemsets, possibly three extensions
            if last_pair_1.item != last_pair_2.item:
                # keep items in ascending order inside the itemset for determinism
                if last_pair_1.item.id <= last_pair_2.item.id:
                    base_pattern = pattern1
                    other_item = last_pair_2.item
                else:
                    base_pattern = pattern2
                    other_item = last_pair_1.item
                new_candidate_equal = base_pattern.clone_pattern()
                new_candidate_equal.add(pair_creator.get_item_abstraction_pair(other_item, qual_creator.create_abstraction_flag(True)))
                candidates.append(new_candidate_equal)

                before_changed = pattern2.clone_pattern()
                before_changed.add(last_pair_1)
                candidates.append(before_changed)
            before_relation = pattern1.clone_pattern()
            before_relation.add(last_pair_2)
            candidates.append(before_relation)
        elif eq1 and eq2:
            # Both last items are in the same itemset -> only one extension
            new_candidate_equal: Optional[Pattern] = None
            if pattern1 < pattern2:
                new_candidate_equal = pattern1.clone_pattern()
                new_candidate_equal.add(pair_creator.get_item_abstraction_pair(last_pair_2.item, qual_creator.create_abstraction_flag(True)))
            elif pattern1 > pattern2:
                new_candidate_equal = pattern2.clone_pattern()
                new_candidate_equal.add(pair_creator.get_item_abstraction_pair(last_pair_1.item, qual_creator.create_abstraction_flag(True)))
            if new_candidate_equal is not None:
                candidates.append(new_candidate_equal)
        else:
            # One item in same itemset, the other in a later one -> single extension
            new_candidate: Optional[Pattern] = None
            if eq1:
                new_candidate = pattern1.clone_pattern()
                new_candidate.add(last_pair_2)
            else:
                new_candidate = pattern2.clone_pattern()
                new_candidate.add(last_pair_1)
            if new_candidate is not None:
                candidates.append(new_candidate)

        return candidates

    def is_candidate_in_sequence(self, finder: "CandidateInSequenceFinder", candidate: Pattern, sequence: Sequence, k: int, i: int, position: List[List[int]]) -> None:
        finder.is_candidate_present_in_sequence(candidate, sequence, k, i, position)

    def generate_size2_candidates(self, creator: "AbstractionCreator", pat1: Pattern, pat2: Pattern) -> List[Pattern]:
        output: List[Pattern] = []
        pattern_creator = PatternCreator.get_instance()
        pair_creator = ItemAbstractionPairCreator.get_instance()

        element1 = pat1.get_ith_element(0)
        element2 = pat2.get_ith_element(0)

        elements_new_pattern1 = [
            element1,
            pair_creator.get_item_abstraction_pair(element2.item, AbstractionQualitative.create(False)),
        ]
        output.append(pattern_creator.create_pattern(elements_new_pattern1))

        if element1 != element2:
            elements_new_pattern2 = [
                element2,
                pair_creator.get_item_abstraction_pair(element1.item, AbstractionQualitative.create(False)),
            ]
            output.append(pattern_creator.create_pattern(elements_new_pattern2))

            if element1 > element2:
                smallest_pair, greater_pair = element1, element2
            else:
                smallest_pair, greater_pair = element2, element1
            elements_new_pattern3 = [
                smallest_pair,
                pair_creator.get_item_abstraction_pair(greater_pair.item, AbstractionQualitative.create(True)),
            ]
            output.append(pattern_creator.create_pattern(elements_new_pattern3))
        return output

    @staticmethod
    def _item_compare(item1: Item, item2: Item) -> int:
        if item1 == item2:
            return 0
        return -1 if item1 < item2 else 1

class Sequences:
    def __init__(self, string: str) -> None:
        self.levels: List[List[Pattern]] = [[]]
        self.number_of_frequent_sequences = 0
        self.string = string

    def __str__(self) -> str:
        sb: List[str] = [self.string]
        for level_index, level in enumerate(self.levels):
            sb.append(f"\n***Level {level_index}***\n")
            for sequence in level:
                sb.append(str(sequence))
                sb.append("\n")
        return "".join(sb)

    def to_string_to_file(self, output_sequence_identifiers: bool = False) -> str:
        sb: List[str] = []
        for level in self.levels:
            for sequence in level:
                sb.append(sequence.to_string_to_file(output_sequence_identifiers))
                sb.append("\n")
        return "".join(sb)

    def add_sequence(self, sequence: Pattern, level_index: int) -> None:
        while len(self.levels) <= level_index:
            self.levels.append([])
        self.levels[level_index].append(sequence)
        self.number_of_frequent_sequences += 1

    def add_sequences(self, sequences: List[Pattern], level_index: int) -> None:
        for pattern in sequences:
            self.add_sequence(pattern, level_index)

    def get_level(self, index: int) -> List[Pattern]:
        return self.levels[index]

    def get_level_count(self) -> int:
        return len(self.levels) - 1

    def get_levels(self) -> List[List[Pattern]]:
        return self.levels

    def size(self) -> int:
        return self.number_of_frequent_sequences

    def sort(self) -> None:
        for level in self.levels:
            level.sort()

    def delete(self, i: int) -> None:
        self.number_of_frequent_sequences -= len(self.levels[i])
        self.levels[i].clear()

    def clear(self) -> None:
        for level in self.levels:
            level.clear()
        self.levels.clear()
        self.number_of_frequent_sequences = 0


class CandidateGeneration:
    def __init__(self) -> None:
        self.last_raw_candidate_count: int = 0

    def generate_candidates(
        self,
        frequent_set: Set[Pattern],
        abstraction_creator: AbstractionCreator,
        indexation_map: Dict[Item, Set[Pattern]],
        k: int,
        min_support_absolute: float,
    ) -> Optional[List[Pattern]]:
        self.last_raw_candidate_count = 0
        candidate_set: List[Pattern] = []
        frequent_list = list(frequent_set)
        pruned_candidates: Optional[List[Pattern]] = None
        if k > 2:
            prefix_map: Dict[Tuple[ItemAbstractionPair, ...], List[Pattern]] = {}
            for pat in frequent_list:
                prefix = tuple(pat.get_elements()[:-1])
                prefix_map.setdefault(prefix, []).append(pat)
            for group in prefix_map.values():
                for i in range(len(group)):
                    for j in range(i, len(group)):
                        for candidate in abstraction_creator.generate_candidates(
                            abstraction_creator, group[i], group[j], min_support_absolute
                        ):
                            candidate_set.append(candidate)
            self.last_raw_candidate_count = len(candidate_set)
            if not candidate_set:
                return None
            pruned_candidates = candidate_set
        elif k == 2:
            pruned_candidates = []
            for i in range(len(frequent_list)):
                for j in range(i, len(frequent_list)):
                    pruned_candidates.extend(
                        abstraction_creator.generate_size2_candidates(abstraction_creator, frequent_list[i], frequent_list[j])
                    )
            self.last_raw_candidate_count = len(pruned_candidates)
        if not pruned_candidates:
            return None
        return pruned_candidates

    def _pruned_subset(self, candidate_set: List[Pattern], frequent_set: Set[Pattern], abstraction_creator: AbstractionCreator) -> List[Pattern]:
        candidate_patterns: List[Pattern] = []
        for candidate in candidate_set:
            is_infrequent = False
            for i in range(len(candidate.get_elements())):
                subpattern = abstraction_creator.get_subpattern(candidate, i)
                if subpattern not in frequent_set:
                    is_infrequent = True
                    break
            if not is_infrequent:
                candidate_patterns.append(candidate)
        return candidate_patterns


class CandidateInSequenceFinder:
    def __init__(self, creator: AbstractionCreator) -> None:
        self.creator = creator
        self.present = False

    def is_candidate_present_in_sequence(self, candidate: Pattern, sequence: Sequence, k: int, length: int, position: List[List[int]]) -> None:
        pair = candidate.get_ith_element(length)
        item_pair = pair.item
        abstraction_pair = pair.abstraction
        previous_abstraction = candidate.get_ith_element(length - 1).abstraction if length > 0 else None
        cancelled = False
        pos: Optional[Tuple[int, int]] = None
        while position[length][0] < sequence.size():
            if length == 0:
                pos = sequence.search_for_first_appearance(position[length][0], position[length][1], item_pair)
            else:
                pos = self.creator.find_position_of_item_in_sequence(
                    sequence,
                    item_pair,
                    abstraction_pair,
                    previous_abstraction,
                    position[length][0],
                    position[length][1],
                    position[length - 1][0],
                    position[length - 1][1],
                )
            if pos is not None:
                position[length] = [pos[0], pos[1]]
                if length + 1 < k:
                    new_pos = self._increase_position(sequence, position[length])
                    position[length + 1] = new_pos
                    self.is_candidate_present_in_sequence(candidate, sequence, k, length + 1, position)
                    if self.present:
                        return
                else:
                    self.present = True
                    return
            else:
                if length > 0:
                    new_pos = self._increase_itemset(position[length - 1])
                    position[length - 1] = new_pos
                cancelled = True
                break
        if length > 0 and not cancelled:
            position[length - 1] = self._increase_itemset(position[length - 1])

    def _increase_position(self, sequence: Sequence, pos: List[int]) -> List[int]:
        if pos[1] < sequence[pos[0]].size() - 1:
            return [pos[0], pos[1] + 1]
        return [pos[0] + 1, 0]

    def _increase_itemset(self, pos: List[int]) -> List[int]:
        return [pos[0] + 1, 0]

class SupportCounting:
    def __init__(self, database: "SequenceDatabase", creator: AbstractionCreator) -> None:
        self.database = database
        self.abstraction_creator = creator
        self.indexation_map: Dict[Item, Set[Pattern]] = {}

    def count_support(self, candidate_set: List[Pattern], k: int, min_support_absolute: float) -> Set[Pattern]:
        self.indexation_map.clear()
        for sequence in self.database.get_sequences():
            self._check_candidate_in_sequence(sequence, k, candidate_set)
        result: Set[Pattern] = set()
        for candidate in candidate_set:
            if candidate.get_support() >= min_support_absolute:
                result.add(candidate)
                self._put_in_indexation_map(candidate)
        return result

    def _check_candidate_in_sequence(self, sequence: Sequence, k: int, candidate_set: List[Pattern]) -> None:
        for candidate in candidate_set:
            position: List[List[int]] = [[0, 0] for _ in range(k)]
            finder = CandidateInSequenceFinder(self.abstraction_creator)
            self.abstraction_creator.is_candidate_in_sequence(finder, candidate, sequence, k, 0, position)
            if finder.present:
                candidate.add_appearance(sequence.id)

    def _put_in_indexation_map(self, entry: Pattern) -> None:
        pair = entry.get_ith_element(0)
        correspondence = self.indexation_map.get(pair.item)
        if correspondence is None:
            correspondence = set()
            self.indexation_map[pair.item] = correspondence
        correspondence.add(entry)

    def get_indexation_map(self) -> Dict[Item, Set[Pattern]]:
        return self.indexation_map


class SequenceDatabase:
    def __init__(self, abstraction_creator: AbstractionCreator) -> None:
        self.abstraction_creator = abstraction_creator
        self.frequent_items: Dict[Item, Pattern] = {}
        self.sequences: List[Sequence] = []
        self.item_factory = ItemFactory()
        self.pattern_creator = PatternCreator.get_instance()

    def load_file(self, path: Path, min_support_relative: float) -> None:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ("#", "%", "@"):
                    continue
                self.add_sequence(line.split(" "))
        min_sup_relative = int(math.ceil(min_support_relative * len(self.sequences)))
        if min_sup_relative == 0:
            min_sup_relative = 1
        items_to_remove: Set[Item] = set()
        for item, pattern in self.frequent_items.items():
            if pattern.get_support() < min_sup_relative:
                items_to_remove.add(item)
        for non_frequent in items_to_remove:
            self.frequent_items.pop(non_frequent, None)
        self._shrink_database(set(self.frequent_items.keys()))

    def add_sequence(self, tokens: List[str]) -> None:
        pair_creator = ItemAbstractionPairCreator.get_instance()
        sequence = Sequence(len(self.sequences))
        itemset = Itemset()
        for token in tokens:
            if token.startswith("<") and token.endswith(">"):
                value = token[1:-1]
                timestamp = int(value)
                itemset.timestamp = timestamp
            elif token == "-1":
                time = itemset.timestamp + 1
                sequence.add_itemset(itemset)
                itemset = Itemset()
                itemset.timestamp = time
            elif token == "-2":
                self.sequences.append(sequence)
            else:
                item = self.item_factory.get_item(int(token))
                pattern = self.frequent_items.get(item)
                if pattern is None:
                    pair = pair_creator.get_item_abstraction_pair(item, self.abstraction_creator.create_default_abstraction())
                    pattern = self.pattern_creator.create_pattern([pair])
                    self.frequent_items[item] = pattern
                pattern.add_appearance(sequence.id)
                itemset.add_item(item)

    def __str__(self) -> str:
        r: List[str] = []
        for sequence in self.sequences:
            r.append(f"{sequence.id}:  {sequence}")
        return "\n".join(r)

    def size(self) -> int:
        return len(self.sequences)

    def get_sequences(self) -> List[Sequence]:
        return self.sequences

    def frequent_items_list(self) -> List[Pattern]:
        cells = list(self.frequent_items.values())
        cells.sort()
        return cells

    def clear(self) -> None:
        self.sequences.clear()
        self.frequent_items.clear()
        self.item_factory.pool.clear()

    def _shrink_database(self, frequent_items: Set[Item]) -> None:
        for sequence in self.sequences:
            i = 0
            while i < len(sequence.itemsets):
                itemset = sequence.itemsets[i]
                j = 0
                while j < len(itemset.items):
                    item = itemset.items[j]
                    if item not in frequent_items:
                        itemset.remove_item(j)
                        sequence.number_of_items -= 1
                    else:
                        j += 1
                if itemset.size() == 0:
                    sequence.remove_itemset(i)
                else:
                    i += 1

class AlgoGSP:
    def __init__(self, min_sup_relative: float, min_gap: float, max_gap: float, window_size: float, abstraction_creator: AbstractionCreator) -> None:
        self.min_sup_relative = min_sup_relative
        self.min_gap = min_gap
        self.max_gap = max_gap
        self.window_size = window_size
        self.abstraction_creator = abstraction_creator
        self.is_sorted = False
        self.patterns: Sequences
        self.start = 0
        self.end = 0
        self.frequent_items: List[Pattern] = []
        self.number_of_frequent_patterns = 0
        self.writer_path: Optional[Path] = None
        self.output_sequence_identifiers = False
        self.join_count: int = 0

    def run_algorithm(
        self,
        database: SequenceDatabase,
        keep_patterns: bool,
        verbose: bool,
        output_file_path: Optional[str],
        output_sequence_identifiers: bool,
    ) -> Sequences:
        self.output_sequence_identifiers = output_sequence_identifiers
        self.patterns = Sequences("FREQUENT SEQUENTIAL PATTERNS")
        self.writer_path = Path(output_file_path) if output_file_path else None
        if self.writer_path is not None:
            self.writer_path.parent.mkdir(parents=True, exist_ok=True)
        self.min_sup_absolute = int(math.ceil(self.min_sup_relative * database.size()))
        if self.min_sup_absolute == 0:
            self.min_sup_absolute = 1
        candidate_generator = CandidateGeneration()
        support_counter = SupportCounting(database, self.abstraction_creator)
        MemoryLogger.get_instance().reset()
        self.start = self._current_time()
        self._run_gsp(database, candidate_generator, support_counter, keep_patterns, verbose)
        self.end = self._current_time()
        return self.patterns

    def _run_gsp(
        self,
        database: SequenceDatabase,
        candidate_generator: CandidateGeneration,
        support_counter: SupportCounting,
        keep_patterns: bool,
        verbose: bool,
    ) -> None:
        self.join_count = 0
        self.frequent_items = database.frequent_items_list()
        self.patterns.add_sequences(self.frequent_items, 1)
        frequent_set: Set[Pattern] = set(self.frequent_items)
        self.number_of_frequent_patterns = len(self.frequent_items)
        indexation_map: Dict[Item, Set[Pattern]] = {}
        k = 1
        while frequent_set:
            k += 1
            if verbose:
                print(f"k={k}")
                print("generating candidates...")
            candidate_set = candidate_generator.generate_candidates(frequent_set, self.abstraction_creator, indexation_map, k, self.min_sup_absolute)
            frequent_set = set()
            if candidate_set is None:
                self.join_count += candidate_generator.last_raw_candidate_count
                break
            self.join_count += candidate_generator.last_raw_candidate_count
            if verbose:
                print(f"{len(candidate_set)} Candidates have been created!")
                print("checking frequency...")
            MemoryLogger.get_instance().check_memory()
            frequent_set = support_counter.count_support(candidate_set, k, self.min_sup_absolute)
            if verbose:
                print(f"{len(frequent_set)} frequent patterns\n")
            MemoryLogger.get_instance().check_memory()
            self.number_of_frequent_patterns += len(frequent_set)
            indexation_map = support_counter.get_indexation_map()
            self.patterns.add_sequences(list(frequent_set), k)
            level = k - 1
            if not keep_patterns:
                if frequent_set:
                    self.patterns.delete(level)
            elif self.writer_path is not None:
                if frequent_set:
                    with self.writer_path.open("a", encoding="utf-8") as writer:
                        for seq in self.patterns.get_level(level):
                            writer.write(seq.to_string_to_file(self.output_sequence_identifiers))
                            writer.write("\n")
                    self.patterns.delete(level)
        if keep_patterns and self.writer_path is not None:
            level = self.patterns.get_level_count()
            if level >= 0 and self.patterns.get_level(level):
                with self.writer_path.open("a", encoding="utf-8") as writer:
                    for seq in self.patterns.get_level(level):
                        writer.write(seq.to_string_to_file(self.output_sequence_identifiers))
                        writer.write("\n")
                self.patterns.delete(level)
        MemoryLogger.get_instance().check_memory()

    def print_statistics(self) -> str:
        if not self.is_sorted:
            self.patterns.sort()
            self.is_sorted = True
        sb = []
        sb.append("=============  Algorithm - STATISTICS =============\n Total time ~ ")
        sb.append(str(self.running_time()))
        sb.append(" ms\n")
        sb.append(" Frequent sequences count : ")
        sb.append(str(self.number_of_frequent_patterns))
        sb.append("\n")
        sb.append(" Join count : ")
        sb.append(str(self.join_count))
        sb.append("\n")
        sb.append(" Max memory (mb):")
        sb.append(str(MemoryLogger.get_instance().get_max_memory()))
        sb.append("\n")
        if self.writer_path is None:
            sb.append(str(self.patterns))
        else:
            sb.append(f" Content at file ./{self.writer_path.name}")
        sb.append("\n")
        sb.append("===================================================\n")
        return "".join(sb)

    def printed_output_to_save_in_file(self) -> str:
        if not self.is_sorted:
            self.patterns.sort()
            self.is_sorted = True
        return self.patterns.to_string_to_file(self.output_sequence_identifiers)

    def get_number_of_frequent_patterns(self) -> int:
        return self.number_of_frequent_patterns

    def get_patterns(self) -> Optional[str]:
        if self.writer_path is None:
            return str(self.patterns)
        return None

    def running_time(self) -> float:
        return self.end - self.start

    def get_min_sup_absolute(self) -> float:
        return self.min_sup_absolute

    @staticmethod
    def _current_time() -> float:
        import time

        return time.time() * 1000

    def clear(self) -> None:
        self.patterns.clear()
        self.frequent_items.clear()
        self.abstraction_creator = None  # type: ignore


def file_to_path(filename: str) -> Path:
    return Path(__file__).with_name(filename)


def format_sequences_for_output(database: SequenceDatabase) -> str:
    lines: List[str] = []
    for idx, sequence in enumerate(database.get_sequences(), start=1):
        parts: List[str] = []
        for itemset in sequence.itemsets:
            items_str = " ".join(str(item) for item in itemset.items)
            # Display timestamps starting from 1 for readability (mirrors Java sample)
            parts.append(f"{{t={itemset.timestamp + 1}, {items_str}}}")
        lines.append(f"{idx}:  " + "".join(parts))
    return "\n".join(lines)


def main(output_path: Optional[Path] = None) -> None:
    support = 0.5
    mingap = 0
    maxgap = float("inf")
    window_size = 0
    keep_patterns = True
    verbose = False
    output_sequence_identifiers = False

    abstraction_creator = AbstractionCreatorQualitative.get_instance()
    sequence_database = SequenceDatabase(abstraction_creator)
    dataset_path = file_to_path("contextPrefixSpan.txt")
    sequence_database.load_file(dataset_path, support)

    # Print database in the same style as the Java sample output
    print(format_sequences_for_output(sequence_database))
    print()

    algo = AlgoGSP(support, mingap, maxgap, window_size, abstraction_creator)
    output_file = output_path or Path(__file__).with_name("spade_output.txt")
    if output_file.exists():
        output_file.unlink()

    algo.run_algorithm(sequence_database, keep_patterns, verbose, str(output_file), output_sequence_identifiers)

    print(f"Minimum support (relative) = {support}")
    print(f"{algo.get_number_of_frequent_patterns()} frequent patterns.")
    print()
    print(algo.print_statistics())


if __name__ == "__main__":
    main()
