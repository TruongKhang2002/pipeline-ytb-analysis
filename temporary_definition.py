# dagster_project/repository.py
from dagster import Definitions, ScheduleDefinition, define_asset_job
from .assets import extract_data, load_data, transform_data

my_job = define_asset_job(name="daily_job", selection="*")
my_schedule = ScheduleDefinition(
    job=my_job,
    cron_schedule="0 8 * * *",  # Chạy lúc 08:00 mỗi ngày
)

defs = Definitions(
    assets=[
        extract_data,
        load_data,
        transform_data
    ],
    schedules=[my_schedule]
)
#=============================================
from dagster import Definitions, load_assets_from_modules
from etl_pipeline import assets
from etl_pipeline import transform_asset
from resources.minio_io_manager import minio_io_manager
from resources.psql_io_manager import psql_io_manager
from dagster_dbt import dbt_cli_resource

all_assets = load_assets_from_modules([assets])
dbt_assets = transform_assets.dbt_assets

defs = Definitions(
    assets=[*all_assets, dbt_assets],
    resources={
        "minio_io_manager": minio_io_manager,
        "psql_io_manager": psql_io_manager,
        "dbt": dbt_cli_resource.configured({
            "project_dir": "youtube_trending",
            "profiles_dir": "."
        }),
        # "dbt_io_manager": dbt_io_manager nếu muốn dùng io_manager riêng
    },
)


from dagster import Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules, SourceAsset
#from .assets import extract_data, load_data, transform_data
from etl_pipeline import assets
from etl_pipeline import transform_asset
from resources.minio_io_manager import minio_io_manager
from resources.psql_io_manager import psql_io_manager
from dagster_dbt import dbt_cli_resource

my_job = define_asset_job(name="daily_job", selection="*")
my_schedule = ScheduleDefinition(
    job=my_job,
    cron_schedule="0 8 * * *",  # Chạy lúc 08:00 mỗi ngày
)

all_assets = load_assets_from_modules([assets])
# Chuyển dbt_assets thành list nếu cần
if isinstance(transform_asset.dbt_assets, list):
    dbt_assets = transform_asset.dbt_assets
else:
    dbt_assets = [transform_asset.dbt_assets]
#source_table_names = ["trending_ca_raw", "trending_de_raw", "trending_fr_raw", "trending_gb_raw", "trending_in_raw", "trending_jp_raw", "trending_kr_raw", "trending_mx_raw", "trending_ru_raw", "trending_us_raw"]

source_assets = [
    SourceAsset(key=["public", f"trending_{country}_raw"])
    for country in ["ca", "de", "fr", "gb", "in", "jp", "kr", "mx", "ru", "us"]
]

defs = Definitions( 
    assets=[*all_assets, *dbt_assets, *source_assets],
    resources={
        "minio_io_manager": minio_io_manager,
        "psql_io_manager": psql_io_manager,
        "dbt": dbt_cli_resource.configured({
            "project_dir": "youtube_trending",
            "profiles_dir": "."
        })
    },
    schedules=[my_schedule]
)
