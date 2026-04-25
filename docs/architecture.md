# Pipeline Architecture

## Overview
End-to-end social media analytics pipeline processing simulated
events through batch ingestion, real-time streaming, cloud storage,
and business intelligence layers.

## Data Flow
Faker (simulation)
│
▼
Bronze Layer (raw Parquet, Hive partitioned)
│
├── Batch: generate_users/posts/interactions.py
└── Stream: Kafka producer → consumer → bronze files
│
▼
DuckDB bronze views (setup_duckdb.py)
│
▼
dbt Core transformations
├── Silver: stg_users, stg_posts, stg_comments, stg_likes
└── Gold: mart_daily_engagement, mart_hashtag_performance,
mart_user_performance
│
├── Local: DuckDB gold tables → HTML dashboard
└── Cloud: S3 upload → Athena SQL queries

## Medallion Architecture
- Bronze: Raw data as received, never modified
- Silver: Cleaned, typed, deduplicated, enriched
- Gold: Aggregated business metrics, dashboard-ready

## Kafka Topics
- social.posts (3 partitions)
- social.likes (3 partitions)
- social.comments (3 partitions)
- social.users (1 partition)

## dbt Models
- stg_users: deduplication, influencer tier classification
- stg_posts: engagement score normalisation, time features
- stg_comments: sentiment standardisation
- stg_likes: reaction type standardisation
- mart_daily_engagement: platform KPIs by day
- mart_hashtag_performance: trending score calculation
- mart_user_performance: RFM-style user segmentation

## AWS Components
- S3 bucket: social-media-pipeline-kashif
- Athena database: social_media_db
- Tables: posts, users
- Query output: s3://bucket/athena-results/