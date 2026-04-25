# Social Media Analytics Pipeline

A production-grade, end-to-end data engineering pipeline for social media analytics. Processes simulated posts, likes, comments, and user events through a complete modern data stack — from real-time Kafka streaming to cloud storage on AWS S3 with SQL analytics via Athena.

---

## Architecture

```
Simulated Events (Python/Faker)
         │
         ▼
┌─────────────────────┐
│    Kafka Topics      │  social.posts / social.likes / social.comments
│    (Real-time)       │  Producer → Broker → Consumer
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Bronze Layer       │  Raw JSONL + Parquet, Hive partitioned
│   Local + S3         │  year=/month=/day=/
└──────────┬──────────┘
           │  dbt Core
           ▼
┌─────────────────────┐
│   Silver Layer       │  Cleaned, typed, deduplicated
│   (dbt staging)      │  stg_users, stg_posts, stg_comments, stg_likes
└──────────┬──────────┘
           │  dbt marts
           ▼
┌─────────────────────┐
│    Gold Layer        │  mart_daily_engagement
│    (dbt marts)       │  mart_hashtag_performance
│                      │  mart_user_performance
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌──────────────────────┐
│  Metabase Dashboard  │     │   AWS S3 + Athena     │
│  localhost:3001      │     │   Cloud analytics     │
└─────────────────────┘     └──────────────────────┘
```

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Data simulation | Python, Faker | Realistic social media events |
| Message broker | Apache Kafka | Real-time event streaming |
| Storage format | Parquet, JSONL | Columnar + raw line-delimited |
| Query engine | DuckDB | Local SQL on files |
| Transformation | dbt Core | SQL models with tests + lineage |
| Stream processing | Python windowed aggregation | Trending topics, sentiment |
| Cloud storage | AWS S3 | Production data lake |
| Cloud analytics | AWS Athena | SQL on S3 at scale |
| Dashboard | Metabase | Business KPI visualization |

---

## Project Structure

```
social-media-pipeline/
├── ingestion/
│   ├── generate_users.py          # User profile simulation
│   ├── generate_posts.py          # Post event simulation
│   ├── generate_interactions.py   # Likes and comments
│   ├── run_ingestion.py           # Full ingestion orchestrator
│   ├── setup_duckdb.py            # Register bronze views in DuckDB
│   ├── upload_to_s3.py            # Upload bronze layer to AWS S3
│   └── query_athena.py            # Run analytics on AWS Athena
├── dbt_project/social_media/
│   ├── models/
│   │   ├── staging/               # Silver layer models
│   │   └── marts/                 # Gold layer models
│   └── dbt_project.yml
├── kafka/
│   ├── docker-compose.yml         # Kafka + Kafka UI
│   ├── setup_topics.py            # Create Kafka topics
│   ├── producer.py                # Live event producer
│   └── consumer.py                # Bronze layer writer
├── spark/
│   └── streaming_job.py           # Windowed aggregations
├── tests/
│   ├── data_quality.py            # Automated quality checks
│   └── export_to_sqlite.py        # Gold → SQLite for Metabase
└── data/                          # All data (gitignored)
    ├── bronze/                    # Raw data
    ├── silver/                    # Cleaned data
    ├── gold/                      # Aggregated KPIs
    └── dashboard.db               # SQLite for Metabase
```

---

## Data Model

### Events simulated
- **1,000 users** — profiles with follower counts, account types, influencer tiers
- **2,000 posts** — with hashtags, engagement metrics, viral detection
- **6,000 comments** — with sentiment labels
- **30,000 likes** — with reaction types (like, love, laugh, wow, sad)

### dbt lineage
```
bronze.users    → stg_users    → mart_user_performance
bronze.posts    → stg_posts    → mart_daily_engagement
                              → mart_hashtag_performance
                              → mart_user_performance
bronze.comments → stg_comments
bronze.likes    → stg_likes
```

### dbt tests
30 automated tests on every `dbt test` run:
- `unique` and `not_null` on all primary keys
- `accepted_values` on status, category, sentiment, reaction_type
- `relationships` — foreign key validation across tables

---

## Getting Started

### Prerequisites
- Python 3.9+
- Docker Desktop
- AWS account (free tier)
- Java 17 (for PySpark)

### Installation

```bash
git clone https://github.com/KASHIF-IJAZ/social-media-pipeline.git
cd social-media-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Run batch pipeline

```bash
# Generate data
python ingestion\run_ingestion.py

# Set up DuckDB
python ingestion\setup_duckdb.py

# Run dbt transformations
cd dbt_project\social_media
dbt run
dbt test

# Export to dashboard
python tests\export_to_sqlite.py
```

### Run streaming pipeline

```bash
# Start Kafka
cd kafka && docker compose up -d

# Window 1 - consumer
python kafka\consumer.py

# Window 2 - producer
python kafka\producer.py

# Window 3 - streaming aggregations
python spark\streaming_job.py
```

### Deploy to AWS

```bash
# Configure AWS credentials
aws configure

# Upload to S3
python ingestion\upload_to_s3.py

# Run Athena analytics
python ingestion\query_athena.py
```

---

## Key Concepts Demonstrated

- **Medallion architecture** — Bronze/Silver/Gold layered data lake
- **Hive partitioning** — Date-based folder structure for query optimization
- **dbt lineage** — Visual dependency graph across all models
- **dbt testing** — 30 automated quality checks on every run
- **Kafka streaming** — Producer/consumer decoupling with offset management
- **Windowed aggregations** — Trending topics using sliding time windows
- **Watermark** — Late data handling in stream processing
- **AWS S3** — Cloud data lake replacing local storage
- **AWS Athena** — Serverless SQL on S3 Parquet files
- **RFM-style segmentation** — Influencer tier classification

---

## Dashboard KPIs

Built in Metabase connected to gold layer SQLite:

- Daily engagement trend (posts, likes, viral count)
- Top 15 trending hashtags by score
- Influencer tier distribution
- Top 10 users by lifetime engagement
- Real-time reaction type distribution

---

## Author

**Kashif Ijaz** — Data Analyst transitioning to Data Engineering

- Project 1: E-Commerce Analytics Pipeline (DuckDB, Parquet, Metabase)
- Project 2: Social Media Analytics Pipeline (Kafka, dbt, AWS, Spark)

GitHub: https://github.com/KASHIF-IJAZ