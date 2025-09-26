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