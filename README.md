# pypatternminer

**pypatternminer** is an open-source Python library for **pattern mining**. It provides a broad, research-oriented, and reproducible framework for major pattern-mining families, including:

- **Itemset mining**
- **High-utility itemset mining**
- **High-utility sequential pattern mining**
- **Sequential pattern mining**

The project is developed with two main goals:

1. to provide a **comprehensive Python framework** covering many influential pattern-mining algorithms in a unified environment;
2. to provide **publicly available code and testing datasets** to support transparent and reproducible research.

## Why pypatternminer?

Pattern mining has produced a large number of important algorithms over the past three decades, but the Python ecosystem remains relatively fragmented. **pypatternminer** aims to help bridge this gap by offering:

- a unified Python environment for multiple pattern-mining families;
- broad algorithmic coverage;
- validation against **SPMF** reference implementations;
- public access to source code and testing datasets;
- a foundation for teaching, benchmarking, and future research.

## Current Coverage

At the current stage, **pypatternminer** includes **144 implemented entries**:

| Category | Implemented |
|---|---:|
| Itemset mining | 42 |
| High-utility itemset mining | 70 |
| High-utility sequential pattern mining | 3 |
| Sequential pattern mining | 29 |
| **Total** | **144** |

Planned future extensions include:

- association rule mining
- sequential rule mining
- sequence prediction
- periodic pattern mining
- episode mining

## Validation

A key design principle of **pypatternminer** is **implementation reliability**.

Each Python implementation is validated against the corresponding **Java implementation in SPMF** using:

- the same input datasets,
- the same parameter settings,
- and the same experimental conditions.

The returned pattern sets and associated values, such as support or utility, are compared to ensure matching outputs.

## Installation

### Install from PyPI

```bash
pip install pypatternminer
