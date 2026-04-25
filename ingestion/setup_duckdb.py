import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "social_media.duckdb"

con = duckdb.connect(str(DB_PATH))

print("Setting up DuckDB bronze views...")

# Create bronze schema
con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
con.execute("CREATE SCHEMA IF NOT EXISTS silver")
con.execute("CREATE SCHEMA IF NOT EXISTS gold")

# Register each bronze source as a view
# dbt will read these views via {{ source('bronze', 'users') }}
con.execute(f"""
    CREATE OR REPLACE VIEW bronze.users AS
    SELECT * FROM read_parquet(
        '{PROJECT_ROOT}/data/bronze/users/users_snapshot.parquet'
    )
""")
print("  ✅ bronze.users view created")

con.execute(f"""
    CREATE OR REPLACE VIEW bronze.posts AS
    SELECT * FROM read_parquet(
        '{PROJECT_ROOT}/data/bronze/posts/**/*.parquet',
        union_by_name=true
    )
""")
print("  ✅ bronze.posts view created")

con.execute(f"""
    CREATE OR REPLACE VIEW bronze.comments AS
    SELECT * FROM read_parquet(
        '{PROJECT_ROOT}/data/bronze/comments/**/*.parquet',
        union_by_name=true
    )
""")
print("  ✅ bronze.comments view created")

con.execute(f"""
    CREATE OR REPLACE VIEW bronze.likes AS
    SELECT * FROM read_parquet(
        '{PROJECT_ROOT}/data/bronze/likes/**/*.parquet',
        union_by_name=true
    )
""")
print("  ✅ bronze.likes view created")

# Verify all views work
print("\nVerifying views...")
for table in ["users", "posts", "comments", "likes"]:
    count = con.execute(
        f"SELECT COUNT(*) FROM bronze.{table}"
    ).fetchone()[0]
    print(f"  bronze.{table}: {count} rows")

con.close()
print("\n✅ DuckDB setup complete")