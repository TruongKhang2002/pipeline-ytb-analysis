import os
import json
import pandas as pd
import boto3
import subprocess
from sqlalchemy import create_engine
from dagster import asset, AssetExecutionContext
from kaggle.api.kaggle_api_extended import KaggleApi
import psycopg2

@asset
def extract_data(context: AssetExecutionContext) -> list[str]:
    """Tải data từ Kaggle và upload lên Minio"""
    download_path = "/tmp/raw"
    os.makedirs(download_path, exist_ok=True)

    # Authenticate Kaggle
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files("datasnaek/youtube-new", path=download_path, unzip=True)
    context.log.info("Đã tải toàn bộ dataset từ Kaggle")

    context.log.info(f"AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID', 'minio')}")
    context.log.info(f"AWS_SECRET_ACCESS_KEY: {os.getenv('AWS_SECRET_ACCESS_KEY', 'minio123')}")
    context.log.info(f"MINIO_ENDPOINT: {os.getenv('MINIO_ENDPOINT', 'http://minio:9000')}")
    # Khởi tạo client MinIO
    s3 = boto3.client("s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        #region_name="us-east-1"
    )

    #bucket = os.getenv("DATALAKE_BUCKET")
    bucket = os.getenv("DATALAKE_BUCKET", "warehouse")
    if not bucket:
        raise ValueError("MINIO bucket chưa được đặt (DATALAKE_BUCKET is missing)")
    context.log.info(f"📦 Using bucket: {bucket}")
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        s3.create_bucket(Bucket=bucket)

    uploaded_countries = []

    for file in os.listdir(download_path):
        if not isinstance(file, str):
            context.log.warning(f"Bỏ qua non-string file: {file}")
            continue
        if file.endswith("videos.csv"):
            country = file[:2].upper()

            if not country.isalpha():
                context.log.warning(f"Bỏ qua file không hợp lệ: {file}")
                continue

            csv_path = os.path.join(download_path, file)
            json_name = f"{country}_category_id.json"
            json_path = os.path.join(download_path, json_name)

            # Upload CSV
            s3.upload_file(csv_path, bucket, f"bronze/{country}/{file}")
            context.log.info(f"⬆️ Uploaded {file} lên MinIO")

            if os.path.exists(json_path):
                s3.upload_file(json_path, bucket, f"bronze/{country}/{json_name}")
                context.log.info(f"⬆️ Uploaded {json_name} lên MinIO")
            else:
                context.log.warning(f"Không tìm thấy {json_name}, sẽ bỏ qua ánh xạ category_name")

            uploaded_countries.append(country)

    return uploaded_countries


@asset (
    name="load_data",
    description="Load data từ MinIO sang PostgreSQL",
    metadata={
        "source": "🪣 MinIO",
        "destination": "🐘 PostgreSQL"
    }
)
def load_data(context: AssetExecutionContext, extract_data: list[str]):
    """Load data vào PostgreSQL"""
    bucket = os.getenv("MINIO_BUCKET", "warehouse")
    s3 = boto3.client("s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"))

    pg_user = os.getenv("POSTGRES_USER", "admin")
    pg_password = os.getenv("POSTGRES_PASSWORD", "admin123")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "postgres")

    conn_info = (
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )
    engine = create_engine(conn_info, future=True)

    for country in extract_data:
        csv_key = f"bronze/{country}/{country}videos.csv"
        json_key = f"bronze/{country}/{country}_category_id.json"
        local_csv = f"/tmp/temp_{country}.csv"
        local_json = f"/tmp/temp_{country}.json"

        # Tải file CSV
        s3.download_file(bucket, csv_key, local_csv)

        # Đọc dữ liệu
        try:
            df = pd.read_csv(local_csv, encoding="utf-8")
        except UnicodeDecodeError as e:
            #context.log.warning(f"UTF-8 decode error tại {local_csv}: {e}. Sử dụng encoding='latin1'")
            df = pd.read_csv(local_csv, encoding="latin1")
        df["country"] = country

        # Nếu có file JSON thì ánh xạ category
        
        try:
            s3.download_file(bucket, json_key, local_json)
            with open(local_json, "r", encoding="utf-8") as f:
                raw = f.read()
                
            categories = json.loads(raw)
            cat_map = {int(item["id"]): item["snippet"]["title"] for item in categories["items"]}
            
            #df["categoryId"] = pd.to_numeric(df["categoryId"], errors="coerce").astype("Int64")
            #df["category_name"] = df["categoryId"].map(cat_map)
            df["categoryId"] = pd.to_numeric(df["category_id"], errors="coerce")
            df["categoryId"] = df["categoryId"].astype("Int64")
            df["category_name"] = df["categoryId"].apply(lambda x: cat_map.get(int(x)) if pd.notna(x) else None)
            

            context.log.info(f"Ánh xạ category cho {country}")
        except:
            context.log.warning(f"Không ánh xạ được category_name cho {country}")
            df["category_name"] = None
        

        # Ghi vào PostgreSQL
        table_name = f"trending_{country.lower()}_raw"
        
        df.to_sql(table_name, engine, if_exists="append", index=False)
        context.log.info(f"Đã insert {len(df)} records vào bảng {table_name}")


@asset
def transform_data(context: AssetExecutionContext, load_data):
    """Chạy DBT transform"""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "./", "--project-dir", "youtube_trending"],
        capture_output=True,
        text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception("DBT run failed")