# resources/minio_io_manager.py

from dagster import IOManager, io_manager
import boto3


class MinioIOManager(IOManager):
    def __init__(self, s3, bucket):
        self.s3 = s3
        self.bucket = bucket

    def handle_output(self, context, obj):
        # Bạn có thể tùy chỉnh cách lưu obj lên MinIO nếu cần
        pass

    def load_input(self, context):
        # Bạn có thể cài đặt nếu muốn load input từ MinIO
        pass


@io_manager(config_schema={"endpoint_url": str, "aws_access_key_id": str, "aws_secret_access_key": str, "bucket": str})
def minio_io_manager(init_context):
    cfg = init_context.resource_config
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
    )
    return MinioIOManager(s3, cfg["bucket"])