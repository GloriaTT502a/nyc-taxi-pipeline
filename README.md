# 🚖 NYC Taxi Pipeline: Production-Grade Lakehouse Architecture

> A robust, end-to-end data engineering pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold). Built with **PySpark, Delta Lake, and dbt** on Databricks, this project processes large-scale geospatial data from the NYC Taxi & Limousine Commission with a strict focus on data quality, spatial optimization, and CI/CD best practices.

---

## 🚀 Key Engineering Features

* **Geospatial Optimization:** Implemented **H3 (Hexagonal Hierarchical Indexing)** to encode lat/long coordinates into 64-bit integers, drastically optimizing spatial joins.
* **Modular Data Quality (DQ):** Developed a decoupled `apply_dq_rules` framework for dynamic validation of business logic prior to Silver layer promotion.
* **Hybrid Engineering Approach:** Seamlessly integrates Python/Spark for heavy-duty ingestion with **dbt** for analytical modeling and data governance.
* **Production-Ready Standard:** Utilizes `src-layout` with Python Namespace Packages, ensuring modularity, testability, and professional distribution.
* **Automated CI/CD & Testing:** Achieved high code coverage via GitHub Actions, using `pytest` and `chispa` for rigorous DataFrame comparisons.

---

## 🏗 Tech Stack

| Category | Technologies |
| :--- | :--- |
| **Compute & Engine** | PySpark (Spark 3.x), Databricks Runtime |
| **Storage & Governance** | Delta Lake, Unity Catalog, AWS S3 / Azure Data Lake |
| **Transformation** | dbt Core |
| **Orchestration & CI/CD** | Databricks Workflows, GitHub Actions |
| **Testing & Quality** | `pytest`, `chispa` (DataFrame testing) |
| **Environment** | Ubuntu, Docker, Databricks CLI |

---

## 📂 Repository Structure

The project decouples the **Ingestion Layer (Python/Spark)** from the **Business Logic Layer (dbt)** to ensure high maintainability.

```text
nyc-taxi-pipeline/
├── .github/workflows/      # Automated CI/CD (Linting, Testing, Deployment)
├── dbt_project/            # 🌟 Analytics Engineering Layer
│   ├── models/             # SQL-based modeling (Staging -> Silver -> Gold)
│   ├── dbt_project.yml     # dbt configuration for Databricks/Unity Catalog
│   └── profiles.yml        # Connection profiles
├── notebooks/              # Databricks Orchestration Entry Points
│   ├── 01_Run_Bronze       # Production jobs (invoking src/ logic)
│   └── dev_scratchpad_h3   # Experimental notebooks for spatial research
├── src/                    # 🌟 Core Engineering Package (Modular Python)
│   └── nyc_taxi_pipeline/
│       ├── spatial/        # GIS & H3 logic; generates optimized lookup tables
│       ├── bronze/         # Ingestion engine; schema enforcement & Delta conversion
│       ├── silver/         # Advanced PySpark transformations & Pandas UDFs
│       ├── common/         # Shared utilities (logging, decorators)
│       └── config/         # Environment settings and DQ rule definitions
├── tests/                  # Robust test suite (Pytest & Chispa)
├── requirements.txt        # Python dependency management
└── setup.py                # Package installation configuration
```

---

## 🛠 Deep Dive: Engineering Decisions

### 1. Performance-First Spatial Engineering (H3)
Traditional point-in-polygon geospatial joins are computationally expensive at scale. By moving spatial processing to the `src/spatial/` module and utilizing **Pandas UDFs backed by Apache Arrow**, the pipeline pre-calculates H3 Hexagonal Hierarchical Indices. This transforms complex spatial queries into simple integer-based joins, reducing downstream query latency by **~40%**.

### 2. The Hybrid "Medallion" Implementation
* **Bronze (PySpark):** Handles messy raw data. PySpark is utilized for its superior capability in raw file ingestion, complex schema enforcement, and initial sanitization.
* **Silver/Gold (dbt):** Once data is validated and structured, `dbt` takes over. This allows analysts to read version-controlled SQL, automatically generated documentation, and clear data lineage.

### 3. Schema Enforcement & DQ Engine
To prevent a "Data Swamp," the pipeline uses a config-driven utility to validate incoming data against strict schemas and rules before it enters the Silver layer.

```python
# Example DQ config implementation
rules = {
    "trip_distance": "val > 0",
    "passenger_count": "val IS NOT NULL",
    "pickup_datetime": "val < current_timestamp()"
}
```

---

## 📊 Analytics Spotlight: The "Golden Hexagons" (dbt - Work In Progress)

The data pipeline acts as the foundation for deep business intelligence. Currently, I am building Gold-layer `dbt` models to uncover the spatial economics of NYC taxis, specifically analyzing **The Volume vs. Efficiency Paradox**.

### The Goal: Identifying High-Yield Zones
To evaluate the true "value" of an H3 cell, we must look beyond gross revenue and calculate **time-to-yield**. Key metrics modeled in dbt include:
* **Average Fare per Trip:** Gross revenue per ride.
* **Efficiency Index ($/min):** Calculated as `Fare / Trip Duration`. This measures how much a driver earns per minute of active work.

### The Core Paradox: Volume $\neq$ Profitability
This analysis highlights a fundamental mismatch in urban logistics: **the busiest areas are often the least profitable for drivers.**

1. **The "Busy" Hotspots (Demand Trap):** Heatmaps show trip volume heavily concentrated in Midtown and Lower Manhattan during peak rush hours (e.g., Friday 6 PM). However, extreme congestion reduces average speeds to a walking pace (5–8 km/h).
2. **The "Golden" Hotspots (Profitability Flow):** High-efficiency cells shift toward the edges of Manhattan, Brooklyn, and Airport corridors (JFK/LGA). These areas allow for "Flow," where drivers can capitalize on distance-based pricing on highways (like the FDR Drive) rather than wait-time pricing.

### The "Efficiency Trap" Explained
By calculating `earnings_per_min`, the models expose the hidden **Congestion Cost**:
* **Wait-Time Pricing:** Taxis charge a significantly lower rate when moving slowly or stopped.
* **Idle Waste:** Spending 20 minutes to travel 1 mile yields a dismal return on time, even with base fares.
* **Opportunity Cost:** Frequent stopping prevents drivers from catching the next high-value fare and increases vehicle wear.

---

## 📈 Future Roadmap

- [ ] Complete the Gold layer `dbt` models for Spatial RFM Analysis and Driver Efficiency.
- [ ] Integrate **Great Expectations** for automated, deep data profiling and anomaly detection.
- [ ] Implement **MLflow** for robust demand forecasting models localized to specific H3 cells.
- [ ] Optimize Databricks SQL Warehouse configurations for advanced cost-efficiency.
