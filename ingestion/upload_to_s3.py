"""
Uploads local bronze layer to AWS S3.
S3 replaces your local data/ folder in production.

The folder structure is identical to local:
  Local:  data/bronze/posts/year=2026/month=04/day=25/batch.parquet
  S3:     s3://bucket/bronze/posts/year=2026/month=04/day=25/batch.parquet

This is called "lifting and shifting" — same architecture, cloud storage.
"""
import boto3
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"E:/de_project/social-media-pipeline")
BRONZE_DIR   = PROJECT_ROOT / "data" / "bronze"
BUCKET_NAME  = "social-media-pipeline-kashif"  # change to your bucket name

s3 = boto3.client("s3", region_name="us-east-1")

def upload_folder(local_path: Path, s3_prefix: str):
    """
    Uploads only Parquet files from a local folder to S3.
    Skips JSONL files — Athena reads Parquet only.
    """
    files = [f for f in local_path.glob("**/*")
             if f.is_file() and f.suffix == ".parquet"]
    total = len(files)

    print(f"  Uploading {total} parquet files from {local_path.name}/...")

    for i, file_path in enumerate(files, 1):
        relative = file_path.relative_to(local_path)
        s3_key   = f"{s3_prefix}/{relative}".replace("\\", "/")
        s3.upload_file(str(file_path), BUCKET_NAME, s3_key)
        print(f"    [{i}/{total}] s3://{BUCKET_NAME}/{s3_key}")

    return total

def upload_bronze_layer():
    print("\n" + "="*55)
    print("  Uploading Bronze Layer to S3")
    print(f"  Bucket: s3://{BUCKET_NAME}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)

    total = 0

    for entity in ["posts", "users", "products", "comments", "likes"]:
        path = BRONZE_DIR / entity
        if path.exists() and list(path.glob("**/*")):
            total += upload_folder(path, f"bronze/{entity}")
        else:
            print(f"  Skipping {entity} (empty or not found)")

    print(f"\n  Total files uploaded: {total}")
    print(f"  S3 location: s3://{BUCKET_NAME}/bronze/")

def verify_s3_upload():
    """List what's in S3 to confirm upload worked."""
    print("\n  Verifying S3 contents...")
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="bronze/"
    )
    objects = response.get("Contents", [])
    print(f"  Files in S3: {len(objects)}")
    for obj in objects[:5]:
        print(f"    s3://{BUCKET_NAME}/{obj['Key']}")
    if len(objects) > 5:
        print(f"    ... and {len(objects) - 5} more")

if __name__ == "__main__":
    upload_bronze_layer()
    verify_s3_upload()
    print("\n✅ Bronze layer uploaded to S3 successfully!")