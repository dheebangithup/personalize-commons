from personalize_commons.dependencies.aws_providers import get_s3_client
from personalize_commons.services.s3_service import S3Service

__s3_service = None


def get_s3_service() -> S3Service:
    """Dependency provider for S3Service (singleton)"""
    global __s3_service

    if __s3_service is None:
        __s3_service = S3Service(client=get_s3_client())

    return __s3_service