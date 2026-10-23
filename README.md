# Ethereum Blockchain Analytics with Apache Spark

Big Data course project — University of Bologna (A.Y. 2025/2026).

This project implements and evaluates an end-to-end distributed Big Data analytics pipeline on Ethereum blockchain data using **Apache Spark (Scala)**. The application processes raw transaction and block records, enriches transactions with sender activity tiers and block metadata, and computes daily aggregated metrics.

To satisfy course requirements, the pipeline was tested both locally on a small sample ($\le 10\text{ MB}$) and deployed remotely on an **AWS EMR cluster** over a large dataset ($> 5\text{ GB}$). Three job variations (one unoptimized baseline and two optimized versions) were benchmarked to analyze the impact of **co-partitioning**, **pre-aggregation**, and **broadcast hash joins** on execution time and shuffle I/O.

---

## Architecture & Analysis Pipeline

Each transaction is enriched with:
1. **Sender Activity Tier** (computed via self-join on `from_address`):
   - **Tier 1**: $\le 1$ transaction (occasional users)
   - **Tier 2**: $2 - 10$ transactions (active users)
   - **Tier 3**: $11 - 100$ transactions (frequent users)
   - **Tier 4**: $> 100$ transactions (bots, exchanges, high-frequency services)
2. **Transaction Type**: Standard EOA transfer vs. Smart Contract call (identified by inspecting the payload `input` field).
3. **Block Metadata**: Block timestamp converted to UTC date (`YYYY-MM-DD`) and `miner` address.

The pipeline computes daily metrics grouped by `(Date, Miner, IsContractCall, Tier)`:
- `TxCount`: Total number of transactions.
- `TotalValueGwei`: Cumulative transaction value in Gwei ($10^{-9}\text{ ETH}$).
- `AvgGasPrice`: Weighted average gas price in Gwei.

---

## Job Implementations

All three implementations produce **identically verified output** while utilizing different execution strategies:

### 1. Job 1: Baseline Unoptimized Execution
- **Self-Join**: Performs a `reduceByKey` on `from_address` to determine sender counts, followed by a standard shuffle-based `.join()` back with the raw transaction RDD.
- **Block Join**: Joins enriched transactions with the block RDD on `block_number` using a wide shuffle join.
- **Aggregation**: Final `reduceByKey` grouped by `(Date, Miner, IsContractCall, Tier)`.
- **Characteristics**: Incurs multiple costly shuffles, transferring large volumes of intermediate data across the network.

### 2. Job 2: Co-Partitioned Self-Join + Pre-Aggregation
- **Co-Partitioning & Caching**: Partitions transactions by `from_address` using a customized `HashPartitioner` and persists the RDD (`MEMORY_AND_DISK`). The per-sender tier aggregation reuses the exact same partitioner, eliminating shuffle overhead during the self-join.
- **Pre-Aggregation**: Pre-aggregates metrics by `(block_number, tier, isContractCall)` *before* joining with blocks, drastically reducing the record count and data volume shuffled into the second join.
- **Characteristics**: Significant reduction in shuffle write volume and executor execution time.

### 3. Job 3: Co-Partitioned Self-Join + Broadcast Hash Join
- **Broadcast Hash Join**: Reuses the co-partitioned self-join from Job 2. Since `blocks.csv` is much smaller than `transactions.csv`, blocks are collected and broadcast to all executor nodes (`sc.broadcast(...)`), replacing the second shuffle join with an in-memory lookup.
- **Characteristics**: Completely eliminates the second shuffle stage, maximizing throughput and minimizing cluster network I/O.

---

## Datasets

The project uses the [Ethereum Blockchain dataset from Kaggle](https://www.kaggle.com/datasets/buryhuang/ethereum-blockchain):

- `transactions.csv`: Raw transactions (hash, block number, from, to, value, gas price, input payload).
- `blocks.csv`: Block metadata (block number, miner address, timestamp).

### Dataset Setup & Preparation
- **Sample Dataset** (`datasets/sample/`): Sized $\le 10\text{ MB}$ (~6.5 MB transactions + ~2.5 MB blocks) for fast local debugging and unit testing.
- **Full Dataset** (`datasets/big/` and S3): Sized $> 5\text{ GB}$ (~5.2 GB transactions + ~0.47 GB blocks, totaling ~5.67 GB) for remote cluster deployment.
- **Dataset Preparation Script**: [prepare_datasets.py](file:///c:/Users/giorg/IdeaProjects/big-data_project/prepare_datasets.py) extracts transactions from `transactions.csv.zip` and builds a consistent sample where all sample blocks correspond to transactions in the sample, ensuring relational integrity.

---

## Repository Structure

```text
big-data_project/
├── build.gradle               # Gradle build configuration with ShadowJar plugin
├── prepare_datasets.py        # Dataset extraction, sizing, and referential sampling
├── analyze_results.py         # Output equivalence validation & analytical plots
├── analyze_history.py         # Spark event log parsing & benchmark comparison
├── datasets/
│   ├── sample/                # Local testing datasets (<= 10 MB)
│   └── big/                   # Full-scale datasets (> 5 GB, gitignored)
├── history/                   # SparkListener event logs from AWS EMR runs
│   ├── history-remote-ethereumJob1
│   ├── history-remote-ethereumJob2
│   └── history-remote-ethereumJob3
├── output/                    # Job CSV outputs and generated benchmark figures
│   ├── output-remote-ethereumJob1.csv
│   ├── output-remote-ethereumJob2.csv
│   ├── output-remote-ethereumJob3.csv
│   ├── ethereum_analytics.png
│   └── benchmark_history.png
└── src/main/scala/
    ├── project/
    │   ├── EthereumJob.scala    # Core Spark application (Jobs 1, 2, 3)
    │   └── EthereumParser.scala # CSV parsing and timestamp conversion helpers
    └── utils/
        ├── Commons.scala        # Path resolution and S3 credentials setup
        └── Config.scala         # S3 bucket and local directory configurations
```

---

## How to Build & Run

### 1. Build the Fat JAR
Compile the project and bundle all dependencies into a JAR:
```bash
./gradlew
```
The resulting JAR will be located at `build/libs/big-data_project-all.jar`.

### 2. Run Locally
Execute jobs on local sample datasets:
```bash
spark-submit \
  --class project.EthereumJob \
  --master "local[*]" \
  build/libs/big-data_project-all.jar local <jobNumber> [numPartitions]
```
Where `<jobNumber>` is `1`, `2`, or `3`, and `numPartitions` defaults to `8`.

### 3. Run on AWS EMR (Remote Cluster)
Submit jobs on the configured EMR cluster:
```bash
spark-submit \
  --class project.EthereumJob \
  --deploy-mode cluster \
  s3://<your-bucket>/big-data_project-all.jar remote <jobNumber> [numPartitions]
```

### 4. Verification and Benchmarking

- **Validate Output Correctness**:
  ```bash
  python analyze_results.py
  ```
  Confirms that Job 1, Job 2, and Job 3 produced identical results and outputs `output/ethereum_analytics.png`.

- **Parse Execution Metrics & Generate Benchmark Charts**:
  ```bash
  python analyze_history.py
  ```
  Parses event logs in `history/` and produces execution summary tables and `output/benchmark_history.png`.