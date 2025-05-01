# Semantic Similarity Classification

## Overview

This project implements a distributed solution for comparing measures of semantic similarity based on the research paper by Ljubešić et al. The system processes Google Syntactic N-Grams data using MapReduce on Amazon Elastic MapReduce (EMR) to build word relationship models and calculate semantic similarities between word pairs.

**Final Grade: 94/100**

## Authors
- May Hadidim (hadadim@post.bgu.ac.il)
- Asaf Hacmon 

## Project Architecture

The system is structured as a multi-step MapReduce workflow:

1. **Step 1:** Build co-occurrence vectors from syntactic n-grams
2. **Step 1.5:** Count feature occurrences across the corpus
3. **SortFeatures:** Select the top features (skipping the 100 most frequent)
4. **Step 2:** Compute association measures (MLE, PMI, t-test)
5. **Step 3:** Calculate vector similarities using various metrics
6. **Step 4:** Perform classification and evaluation using Weka

### Implementation Details

#### Step 1: Build Co-occurrence Vectors
- **Input:** Google Syntactic N-Grams corpus
- **Process:** Extracts syntactic relationships between words
- **Output:** Word-feature co-occurrence maps (MyHashMapWritable)

#### Step 1.5: Count Features
- **Input:** Step 1 output
- **Process:** Aggregates feature occurrences across all words
- **Output:** Global feature counts

#### SortFeatures: Feature Selection
- **Input:** Step 1.5 output
- **Process:** Sorts features by frequency and selects top 1000 (excluding top 100)
- **Output:** Selected features list with counts

#### Step 2: Association Measures
- **Input:** Step 1 output and SortFeatures output
- **Process:** Calculates association metrics (raw frequency, MLE, PMI, t-test)
- **Output:** Word vectors with different association measures

#### Step 3: Vector Similarities
- **Input:** Step 2 output
- **Process:** Computes 24 similarity metrics between word pairs
- **Output:** CSV with word pairs and similarity values

#### Step 4: Classification
- **Input:** Step 3 output and gold standard dataset
- **Process:** Trains a Naive Bayes classifier using WEKA
- **Output:** Classification metrics (precision, recall, F1)

## Required Libraries

- Apache Hadoop (3.3.1)
- AWS SDK for Java
- WEKA Machine Learning
- SLF4J Logging

## Key Components

### MyHashMapWritable
Custom Writable implementation for storing word-feature co-occurrence data efficiently.

### Association Measures
Four methods implemented:
- Raw frequency
- Maximum Likelihood Estimation (MLE)
- Pointwise Mutual Information (PMI)
- T-test

### Similarity Metrics
Six similarity metrics calculated for each association measure:
- Manhattan distance
- Euclidean distance
- Cosine similarity
- Jaccard similarity
- Dice coefficient
- Jensen-Shannon divergence

### Classification
Uses WEKA's Naive Bayes classifier with 10-fold cross-validation.

## Performance Analysis

The system was tested on:
- 10% of the corpus (10 files)
- 100% of the corpus (all 100 files)

### Communication Statistics
Map-Reduce communication statistics available in the project report.

### Classification Results
Full performance metrics (precision, recall, F1) available in the output of Step 4.

## Project Structure

```
src/main/java/
├── App.java                  # Main application orchestrating EMR
├── ApplicationConfig.java    # Configuration settings
├── AssociationWritable.java  # Writable for association measures
├── FeatureArray.java         # Top features management
├── GS.java                   # Gold Standard dataset accessor
├── MyHashMapWritable.java    # Custom HashMap implementation for Hadoop
├── SortFeatures.java         # Feature selection step
├── Stemmer.java              # Porter Stemmer implementation
├── Step1.java                # Build co-occurrence vectors
├── Step1_5.java              # Count feature occurrences
├── Step2.java                # Compute association measures
├── Step3.java                # Calculate vector similarities
└── Step4.java                # Classification and evaluation
```

## Notes

- The system uses a Porter Stemmer to normalize words before processing
- Word pairs from the gold standard are used both for training and testing (with cross-validation)
- The system disregards the top 100 most frequent features to reduce noise
- Performance scales with the number of EMR instances
- Results may vary based on the specific corpus files used

## Debugging

- Check EMR logs for errors in each step
- EMR console provides detailed job tracking
- Set logging level in the application for more verbose output
- Common issues include S3 permission problems and memory limitations

## References

This project is based on the paper "Comparing Measures of Semantic Similarity" by Ljubešić et al. with modifications as specified in the assignment instructions.
