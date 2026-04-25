# Project 2 — Social Media Analytics Pipeline
## Complete Explanation & Demo Guide

---

# PART 1 — COMPLETE PROJECT UNDERSTANDING

---

## What problem does this project solve?

Imagine you work at a company like Twitter/X. Every second, thousands
of users are posting, liking, and commenting. Your job as a data
engineer is to:

1. Collect all those events reliably
2. Clean the messy raw data into trustworthy tables
3. Aggregate it into business metrics (trending topics, top users)
4. Store it in the cloud so anyone can query it
5. Visualize it so the business can make decisions

That is exactly what this pipeline does — but with simulated data
instead of real Twitter data.

---

## The full story — data flowing through the pipeline

```
STEP 1: Data is born
Python scripts using Faker library create realistic fake social media
events — users, posts, likes, comments. Think of this as your
application backend generating events.

STEP 2: Events stream through Kafka
Real-time events flow through Kafka topics.
Producer sends events → Kafka stores them → Consumer reads them.
This is how every major tech company handles real-time data.

STEP 3: Raw data lands in Bronze layer
Consumer writes events to Parquet files on disk.
Organized by date: year=2026/month=04/day=25/
This is your raw data vault — never modified, always preserved.

STEP 4: dbt cleans and transforms
dbt reads bronze Parquet files and runs SQL transformations.
Bronze → Silver: clean, deduplicate, add derived columns
Silver → Gold:   aggregate into business metrics

STEP 5: Streaming aggregations
Python windowed aggregator reads bronze data and computes trending
hashtags, reaction counts, and sentiment distribution using time windows.

STEP 6: Cloud storage on AWS
Gold layer Parquet files uploaded to AWS S3.
Athena runs SQL queries directly on S3 files — same SQL you wrote
locally, now on Amazon's servers.

STEP 7: Dashboard
HTML dashboard with Chart.js shows all KPIs visually — trending
hashtags, daily engagement, influencer tiers, top users.
```

---

## Every tool explained simply

**Python + Faker**
Generates 1000 users, 2000 posts, 6000 comments, 30000 likes.
In production this would be your actual app sending real events.

**Apache Kafka**
A message queue. Think of it like a conveyor belt. Producer puts items
on the belt, consumer picks them up. The belt keeps moving even if the
consumer is temporarily offline — nothing is lost.

**Bronze layer**
Raw data exactly as received. Like a security camera recording — you
never edit the footage, you only watch it.

**dbt Core**
You write SELECT statements, dbt handles everything else. It reads your
SQL, creates tables, runs tests, tracks which model depends on which,
and generates documentation with a visual lineage graph.
This is the most in-demand DE tool right now.

**Silver layer**
Cleaned data. Nulls removed, duplicates eliminated, types fixed,
derived columns added. Safe to query and trust.

**Gold layer**
Aggregated business answers. Instead of 2000 raw posts you have one row
per day showing total posts, likes, viral count, engagement rate.

**Windowed aggregations**
Instead of counting all-time hashtags, you count hashtags from the last
1 hour. The window slides every 30 seconds — this is how trending topics
work on real platforms.

**AWS S3**
Amazon cloud storage. Identical to your local data/ folder but
accessible from anywhere, virtually unlimited size, and
99.999999999% durable.

**AWS Athena**
SQL engine that runs directly on S3 Parquet files. No database server
needed. Write SQL, it scans S3 files and returns results.
Pay only for data scanned — our usage costs $0.00.

**DuckDB**
The local equivalent of Athena. Runs SQL directly on Parquet files
without any server. Used throughout the pipeline for transformations
and verification.

---

## The data model

```
TABLE                    ROWS      PURPOSE
──────────────────────────────────────────────────────────────
bronze.users             1,000     one row per user profile
bronze.posts             2,000     one row per post published
bronze.comments          6,000     one row per comment written
bronze.likes            30,000     one row per like/reaction

silver.stg_users         ~1,000    cleaned users + influencer_tier
silver.stg_posts         ~1,960    cleaned posts (deleted removed)
silver.stg_comments      ~5,820    cleaned comments
silver.stg_likes        ~30,000    cleaned likes

gold.mart_daily_engagement    ~89  one row per day
gold.mart_hashtag_performance  ~30 one row per hashtag
gold.mart_user_performance  ~1,000 one row per user
```

Every table builds on the previous one.
Gold is the final answer the business actually uses.

---

## The 3 most important concepts

**1. Medallion architecture**
Bronze/Silver/Gold. Every professional data lake uses this pattern.
Bronze = raw truth, Silver = clean trust, Gold = business value.

**2. dbt lineage**
Run dbt docs serve and you see a visual graph showing exactly how every
table connects. If something breaks you know immediately which
downstream tables are affected.

**3. Kafka decoupling**
Producer and consumer are completely independent. Your app does not care
how many consumers are reading its events. You can add fraud detection,
notifications, and analytics consumers — all reading the same events —
without changing the producer at all.

---

## Project folder structure explained

```
social-media-pipeline/
│
├── ingestion/                   DATA GENERATION + INGESTION
│   ├── generate_users.py        Creates 1000 fake user profiles
│   ├── generate_posts.py        Creates 2000 fake posts with hashtags
│   ├── generate_interactions.py Creates 6000 comments + 30000 likes
│   ├── run_ingestion.py         Runs all three generators in order
│   ├── setup_duckdb.py          Registers bronze Parquet as DuckDB views
│   ├── verify_bronze.py         Confirms bronze data looks correct
│   ├── upload_to_s3.py          Uploads bronze Parquet files to AWS S3
│   └── query_athena.py          Runs analytics queries on AWS Athena
│
├── dbt_project/social_media/    SQL TRANSFORMATIONS
│   ├── models/staging/          Silver layer — 4 cleaning models
│   │   ├── stg_users.sql        Cleans users, adds influencer_tier
│   │   ├── stg_posts.sql        Cleans posts, normalizes engagement
│   │   ├── stg_comments.sql     Cleans comments, standardizes sentiment
│   │   └── stg_likes.sql        Cleans likes, standardizes reaction_type
│   ├── models/marts/            Gold layer — 3 aggregation models
│   │   ├── mart_daily_engagement.sql     Platform KPIs by day
│   │   ├── mart_hashtag_performance.sql  Trending scores per hashtag
│   │   └── mart_user_performance.sql     RFM-style user segmentation
│   └── models/sources.yml       Points dbt to bronze DuckDB views
│
├── kafka/                       REAL-TIME STREAMING
│   ├── docker-compose.yml       Starts Kafka broker + Kafka UI
│   ├── setup_topics.py          Creates 4 Kafka topics
│   ├── producer.py              Simulates live events at 3/second
│   └── consumer.py              Reads events → writes to bronze layer
│
├── spark/                       STREAM PROCESSING
│   └── streaming_job.py         Windowed aggregations on bronze data
│                                (trending hashtags, reactions, sentiment)
│
├── tests/                       QUALITY + DASHBOARD
│   ├── data_quality.py          18 automated checks on silver + gold
│   ├── export_to_sqlite.py      Exports gold to SQLite for Metabase
│   ├── create_dashboard.py      Generates HTML dashboard with Chart.js
│   └── verify_dbt_output.py     Queries dbt output to confirm results
│
├── docs/
│   └── architecture.md          Detailed architecture documentation
│
├── .gitignore                   Excludes data/, venv/, generated files
├── requirements.txt             All Python dependencies with versions
└── README.md                    Professional project documentation
```

---

# PART 2 — HOW TO DEMONSTRATE TO YOUR SENIOR

---

## 15-minute demo script

Follow this exact sequence. Each section has what to do and what to say.

---

### Section 1 — GitHub (2 minutes)

OPEN: https://github.com/KASHIF-IJAZ/social-media-pipeline

SAY:
"This is an end-to-end data engineering pipeline for social media
analytics. It covers the complete modern data stack — real-time
streaming, SQL transformations, automated testing, cloud storage,
and business intelligence.

Let me scroll through the README — you can see the architecture
diagram, tech stack, data model, and setup instructions. This is
how any engineer joining the project would get up to speed."

SHOW: Scroll through README, point out architecture diagram and
tech stack table.

---

### Section 2 — Data ingestion (2 minutes)

OPEN: PowerShell

RUN:
```
cd E:\de_project\social-media-pipeline
.\venv\Scripts\activate
python ingestion\run_ingestion.py
```

SAY:
"This simulates our data sources — 1000 user profiles, 2000 posts,
6000 comments, and 30000 likes. In production this would be our
actual application backend sending real events. The data lands in
the bronze layer as Parquet files partitioned by date — year, month,
day — which is the industry standard Hive partitioning pattern."

---

### Section 3 — Kafka streaming (3 minutes)

OPEN: Two PowerShell windows side by side

Window 1 RUN: python kafka\consumer.py
Window 2 RUN: python kafka\producer.py

SAY:
"Now watch — the producer on the right is simulating live events
happening right now — posts being published, likes being clicked,
comments being written. The consumer on the left is reading those
events from Kafka and writing them to our bronze layer in real time.

The key concept here is decoupling. The producer has no idea the
consumer even exists. I could add three more consumers — fraud
detection, push notifications, a recommendation engine — all reading
the same events, without changing the producer at all. This is why
every major tech company uses Kafka."

OPEN BROWSER: http://localhost:8090
SHOW: Kafka UI with topics and message counts arriving live.

---

### Section 4 — dbt transformations (3 minutes)

RUN:
```
cd dbt_project\social_media
dbt run
dbt test
```

SAY:
"dbt takes our bronze raw data through two transformation layers.
Watch — it runs 7 models in dependency order automatically.
Staging models clean the data, mart models aggregate it into
business metrics.

Now the tests — 30 automated checks run on every execution.
Uniqueness, null checks, foreign key validation, accepted values.
If any check fails the pipeline stops before bad data reaches
the dashboard.

Let me show you the lineage graph."

RUN: dbt docs serve
OPEN: http://localhost:8080

SAY:
"This is automatically generated by dbt. You can see exactly how
every table connects. If I change stg_posts, I immediately know
that mart_daily_engagement, mart_hashtag_performance, and
mart_user_performance will all be affected downstream."

---

### Section 5 — AWS cloud (2 minutes)

OPEN: https://console.aws.amazon.com

SHOW S3:
Navigate to S3 → social-media-pipeline-kashif → bronze/
SAY: "This is our cloud data lake. Same folder structure as local —
bronze, silver, gold — but now on Amazon's infrastructure, accessible
from anywhere, virtually unlimited scale."

SHOW ATHENA:
Navigate to Athena → Query editor → run this query:
```sql
SELECT post_type,
       COUNT(*) AS total_posts,
       AVG(CAST(like_count AS double)) AS avg_likes
FROM posts
GROUP BY post_type
ORDER BY avg_likes DESC
```

SAY: "Athena runs SQL directly on S3 Parquet files. No database
server, no setup, no maintenance. The exact same SQL I write locally
with DuckDB now runs on Amazon's servers. This scales to billions
of rows without changing a single line of code."

---

### Section 6 — Dashboard (1 minute)

RUN:
```
cd E:\de_project\social-media-pipeline
python tests\create_dashboard.py
start data\dashboard.html
```

SAY:
"This is the final output — business-ready metrics that any
stakeholder can understand. KPI cards at the top showing total
posts, likes, comments, views, and viral posts. Daily engagement
trend, trending hashtags, influencer tier distribution, and top
users ranked by lifetime engagement.

The entire pipeline from raw events to this dashboard runs
end-to-end with a single command."

---

## Answers to questions your senior might ask

Q: "Why Kafka instead of writing directly to files?"
A: "Kafka decouples producers from consumers completely. Multiple
systems can consume the same events independently. It also guarantees
no data loss — if a consumer goes down, it resumes from its last
offset when it comes back up."

Q: "Why dbt instead of Python scripts for transformations?"
A: "dbt gives us SQL models with automatic dependency ordering,
built-in testing, and generated documentation. When I run dbt test,
30 checks run automatically. When I run dbt docs serve I see a visual
lineage graph. Python scripts have none of this built in."

Q: "Why Parquet instead of CSV?"
A: "Parquet is columnar storage. If I query only total_amount from
10 million rows, Parquet reads only that column from disk. CSV reads
every column regardless. For analytical workloads this makes queries
10 to 100 times faster with 5 to 10 times better compression."

Q: "Why Athena instead of a traditional database?"
A: "Athena is serverless — no server to manage, no database to
maintain. You pay only for data scanned. Our entire project costs
zero dollars on AWS free tier. A traditional database running 24/7
would cost money every month even when not in use."

Q: "What would you do differently in production?"
A: "Replace simulated data with real API connections. Use Apache
Airflow for automated scheduling instead of manual runs. Use AWS MSK
for managed Kafka instead of local Docker. Add GitHub Actions CI/CD
to run dbt tests on every commit automatically."

Q: "How does this project demonstrate data engineering skills?"
A: "It covers the complete data engineering lifecycle — ingestion,
streaming, batch transformation, data quality, cloud storage, and
serving. Every tool used — Kafka, dbt, Parquet, S3, Athena — is
production-grade and used at companies like Uber, Airbnb, and Netflix."

---

## One-sentence summary for your resume

Built an end-to-end social media analytics pipeline processing 39,000+
events through Apache Kafka streaming, dbt Core transformations with
30 automated tests and lineage tracking, Python windowed aggregations
for real-time trending topic detection, AWS S3 data lake with Athena
SQL analytics, and an interactive KPI dashboard — demonstrating the
complete modern data engineering stack.

---

## Commands quick reference — run in order

```bash
# 1. Generate data
python ingestion\run_ingestion.py

# 2. Set up DuckDB views
python ingestion\setup_duckdb.py

# 3. Start Kafka
cd kafka && docker compose up -d && cd ..

# 4. Stream events (two terminals)
python kafka\consumer.py       # terminal 1
python kafka\producer.py       # terminal 2

# 5. Run dbt transformations
cd dbt_project\social_media
dbt run
dbt test
dbt docs serve                 # opens lineage graph at localhost:8080
cd ..\..

# 6. Run quality checks
python tests\data_quality.py

# 7. Upload to AWS
python ingestion\upload_to_s3.py
python ingestion\query_athena.py

# 8. Generate dashboard
python tests\create_dashboard.py
start data\dashboard.html
```