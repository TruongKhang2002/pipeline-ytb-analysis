import os
from dagster_dbt import load_assets_from_dbt_manifest
from dagster import SourceAsset
import json

# Danh sách các bảng đã load vào DB
'''
source_table_names = [
    "trending_ca_raw", "trending_de_raw", "trending_fr_raw", "trending_gb_raw",
    "trending_in_raw", "trending_jp_raw", "trending_kr_raw", "trending_mx_raw",
    "trending_ru_raw", "trending_us_raw"
]

source_all_countries = [
    SourceAsset(key=["public", table]) for table in source_table_names
]
'''

# Đường dẫn đến dbt manifest
dbt_project_path = os.path.join(os.path.dirname(__file__), "../youtube_trending")
manifest_path = os.path.join(dbt_project_path, "target", "manifest.json")

with open(manifest_path, "r", encoding="utf-8") as f:
    manifest = json.load(f)

dbt_assets = load_assets_from_dbt_manifest(
    manifest=manifest,
    #manifest_path=manifest_path,
    key_prefix=["dbt"],
    #source_assets=source_all_countries
)

'''
from dagster import SourceAsset
from dagster_dbt import load_assets_from_dbt_project
import os

#source_table_names = ["trending_ca_raw", "trending_de_raw", "trending_fr_raw", "trending_gb_raw", "trending_in_raw", "trending_jp_raw", "trending_kr_raw", "trending_mx_raw", "trending_ru_raw", "trending_us_raw"]
#source_all_countries = SourceAsset(key=["public", table]) for table in source_table_names

dbt_assets = load_assets_from_dbt_project(
    project_dir=os.path.join(os.path.dirname(__file__), "../youtube_trending"),
    profiles_dir=os.path.join(os.path.dirname(__file__), "../"),
    key_prefix=["dbt"]
)
'''