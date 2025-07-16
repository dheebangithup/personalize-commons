from personalize_commons.dependencies.aws_providers import get_s3_client
from personalize_commons.dependencies.repositories_provider import get_user_repository
from personalize_commons.services.s3_service import S3Service
from personalize_commons.services.user_service import UserService

__s3_service = None


def get_s3_service() -> S3Service:
    """Dependency provider for S3Service (singleton)"""
    global __s3_service

    if __s3_service is None:
        __s3_service = S3Service(client=get_s3_client())

    return __s3_service

def get_user_service() -> UserService:
    """Dependency provider for UserService (singleton)"""
    global _user_service

    if _user_service is None:
        _user_service = UserService(user_repo=get_user_repository())

    return _user_service
