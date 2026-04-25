"""
Queries S3 data directly using AWS Athena.
Athena = DuckDB but for S3 data at any scale.
Same SQL syntax you already know.
"""
import boto3
import time
import pandas as pd

BUCKET_NAME    = "social-media-pipeline-kashif"
ATHENA_DB      = "social_media_db"
ATHENA_OUTPUT  = f"s3://{BUCKET_NAME}/athena-results/"
REGION         = "us-east-1"

athena = boto3.client("athena", region_name=REGION)

def run_query(sql: str, description: str = "") -> pd.DataFrame:
    """
    Runs a SQL query on Athena and returns results as DataFrame.
    Athena is asynchronous — you submit a query then poll for results.
    """
    if description:
        print(f"\n  Running: {description}")

    # Submit query
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_id = response["QueryExecutionId"]

    # Poll until complete
    while True:
        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ["FAILED", "CANCELLED"]:
            reason = athena.get_query_execution(
                QueryExecutionId=query_id
            )["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Query {status}: {reason}")

        time.sleep(1)

    # Get results
    results  = athena.get_query_results(QueryExecutionId=query_id)
    rows     = results["ResultSet"]["Rows"]

    if len(rows) <= 1:
        return pd.DataFrame()

    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    data    = [
        [col.get("VarCharValue", "") for col in row["Data"]]
        for row in rows[1:]
    ]
    return pd.DataFrame(data, columns=headers)

def setup_athena_database():
    """Create Athena database and tables pointing to S3."""
    print("Setting up Athena database...")

    # Create database
    run_query(
        f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB}",
        "Creating database"
    )

    # Create posts table
    run_query(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.posts (
            post_id       string,
            user_id       string,
            content       string,
            post_type     string,
            hashtags      string,
            hashtag_count int,
            like_count    int,
            comment_count int,
            view_count    int,
            is_viral      boolean,
            post_date     string,
            ingested_at   string
        )
        STORED AS PARQUET
        LOCATION 's3://{BUCKET_NAME}/bronze/posts/'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """, "Creating posts table")
    print("  ✅ posts table created")

    # Create users table
    run_query(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.users (
            user_id          string,
            username         string,
            display_name     string,
            account_type     string,
            follower_count   int,
            following_count  int,
            is_verified      boolean,
            joined_at        string,
            ingested_at      string
        )
        STORED AS PARQUET
        LOCATION 's3://{BUCKET_NAME}/bronze/users/'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """, "Creating users table")
    print("  ✅ users table created")

def run_analytics():
    """Run analytics queries on S3 data via Athena."""
    print("\n" + "="*55)
    print("  Athena Analytics on S3 Data")
    print("="*55)

    # Top hashtags
    df = run_query(f"""
        SELECT
            hashtag_tag,
            COUNT(*) AS post_count
        FROM (
            SELECT
                split(regexp_replace(hashtags, '[\\[\\]"]', ''), ',') AS tags
            FROM {ATHENA_DB}.posts
            WHERE hashtag_count > 0
        )
        CROSS JOIN UNNEST(tags) AS t(hashtag_tag)
        WHERE hashtag_tag != ''
        GROUP BY hashtag_tag
        ORDER BY post_count DESC
        LIMIT 10
    """, "Top 10 hashtags")
    print("\n  Top hashtags:")
    print(df.to_string(index=False))

    # Engagement by post type
    df = run_query(f"""
        SELECT
            post_type,
            COUNT(*) AS total_posts,
            AVG(CAST(like_count AS double)) AS avg_likes,
            AVG(CAST(view_count AS double)) AS avg_views
        FROM {ATHENA_DB}.posts
        GROUP BY post_type
        ORDER BY avg_likes DESC
    """, "Engagement by post type")
    print("\n  Engagement by post type:")
    print(df.to_string(index=False))

    # User verification stats
    df = run_query(f"""
        SELECT
            account_type,
            COUNT(*) AS users,
            SUM(CASE WHEN is_verified = true THEN 1 ELSE 0 END) AS verified
        FROM {ATHENA_DB}.users
        GROUP BY account_type
        ORDER BY users DESC
    """, "Users by account type")
    print("\n  Users by account type:")
    print(df.to_string(index=False))


if __name__ == "__main__":
    setup_athena_database()
    run_analytics()
    print("\n✅ Athena queries complete!")