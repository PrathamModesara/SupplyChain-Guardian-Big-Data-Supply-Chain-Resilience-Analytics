# SupplyChain Guardian
## Big Data Supply Chain Resilience Analytics

A Big Data analytics platform for analyzing supply chain performance, identifying operational risks, detecting critical suppliers, warehouses and routes, and measuring supply chain network resilience using Hadoop HDFS, Apache Spark, Spark SQL and GraphX.

---

## 1. Project Overview

Modern supply chains generate large volumes of operational data from orders, customers, suppliers, warehouses, distribution centers, transportation routes and delivery operations.

The main challenge is not only storing this data, but also transforming it into useful information for identifying:

- Supplier risks
- Warehouse inventory risks
- Transportation route risks
- Delivery delays
- Distribution center bottlenecks
- Network-critical nodes
- Overall supply chain risk
- Supply chain resilience

**SupplyChain Guardian** addresses this problem using a Big Data architecture based on Hadoop HDFS, Apache Spark, Spark SQL and GraphX.

The system processes a large supply-chain dataset and generates analytical outputs in Parquet format inside HDFS.

---

# 2. Business Problem

Supply chain organizations need to identify potential disruptions before they significantly affect operations.

Common problems include:

- Supplier performance degradation
- Late deliveries
- Inventory shortages
- Warehouse capacity problems
- High-risk transportation routes
- Transportation disruptions
- Critical network nodes
- Operational bottlenecks

Traditional analytics may become difficult when the volume of supply-chain data becomes large.

Therefore, the project implements a scalable Big Data pipeline that can process supply-chain records using distributed data processing technologies.

---

# 3. Project Objectives

The main objectives are:

1. Store large supply-chain datasets using Hadoop HDFS.
2. Process raw data using Apache Spark.
3. Perform analytical transformations using Spark SQL.
4. Calculate supplier risk.
5. Calculate warehouse inventory risk.
6. Calculate transportation route risk.
7. Analyze delivery performance.
8. Build a supply-chain network using GraphX.
9. Calculate graph degree and PageRank.
10. Identify critical network nodes.
11. Calculate a resilience score.
12. Store analytical results in HDFS as Parquet files.
13. Generate executive-level supply-chain risk indicators.

---

# 4. Key Technologies

| Technology | Purpose |
|---|---|
| Python | Data processing and Spark applications |
| Scala | Spark and GraphX network analytics |
| Hadoop HDFS | Distributed storage |
| Apache Spark | Big Data processing |
| Spark SQL | Structured analytics |
| GraphX | Supply-chain graph analytics |
| Parquet | Analytical storage format |
| Git | Version control |
| GitHub | Source-code repository |
| WSL Ubuntu | Development environment |

---

# 5. Dataset

The project uses a master supply-chain dataset containing operational information related to:

- Orders
- Customers
- Products
- Suppliers
- Warehouses
- Distribution centers
- Routes
- Transportation
- Delivery performance
- Disruption events
- Supplier performance

The master dataset contains approximately **205,787 rows** including the header.

The raw CSV is intentionally not stored in this GitHub repository because it is a large dataset.

Instead, the dataset is uploaded into HDFS.

---

# 6. Big Data Architecture

```text
                    SUPPLY CHAIN MASTER DATA
                              |
                              v
                    +-------------------+
                    |     Hadoop HDFS   |
                    |       RAW DATA    |
                    +-------------------+
                              |
                              v
                    +-------------------+
                    |   Apache Spark    |
                    | Data Processing   |
                    +-------------------+
                              |
                              v
                    +-------------------+
                    |    Spark SQL      |
                    |   Analytics       |
                    +-------------------+
                              |
              +---------------+----------------+
              |               |                |
              v               v                v
       Supplier Risk    Warehouse Risk    Route Risk
              |               |                |
              +---------------+----------------+
                              |
                              v
                    +-------------------+
                    |      GraphX       |
                    | Network Analysis  |
                    +-------------------+
                              |
                              v
                    +-------------------+
                    | Resilience        |
                    | Analytics         |
                    +-------------------+
                              |
                              v
                    +-------------------+
                    | HDFS Parquet      |
                    | Analytical Output |
                    +-------------------+
