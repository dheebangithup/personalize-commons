# src/dependencies/repositories.py

from personalize_commons.repositories.campaign_repository import CampaignRepository
from personalize_commons.repositories.interaction_tracking_repository import InteractionTrackingRepository
from personalize_commons.repositories.item_repository import ItemRepository
from personalize_commons.repositories.recommendation_repository import RecommendationRepository
from personalize_commons.repositories.tenant_repository import TenantRepository
from personalize_commons.repositories.user_repository import UserRepository

from personalize_commons.dependencies.aws_providers import get_dynamodb_resource, get_dynamodb_client
from personalize_commons.repositories.whatsapp_account_repository import WhatsAppAccountRepository
from personalize_commons.repositories.whatsapp_job_repository import WhatsAppJobRepository
from personalize_commons.repositories.media_file_repository import MediaFileRepository

# Create singleton instances
__user_repository = None
__campaign_repository = None
__item_repository = None
__recommendation_repository = None
__tenant_repository = None
__interaction_repository = None
__whatsapp_account_repository = None
__whatsapp_job_repository = None
__media_file_repository = None


def get_tenant_repository():
    global __tenant_repository
    if __tenant_repository is None:
        __tenant_repository = TenantRepository(resource=get_dynamodb_resource())
    return __tenant_repository

def get_user_repository():
    global __user_repository
    if __user_repository is None:
        __user_repository = UserRepository(client=get_dynamodb_client(),resource=get_dynamodb_resource())
    return __user_repository


def get_campaign_repository():
    global __campaign_repository
    if __campaign_repository is None:
        __campaign_repository = CampaignRepository(resource=get_dynamodb_resource())
    return __campaign_repository

def get_item_repository():
    global __item_repository
    if __item_repository is None:
        __item_repository = ItemRepository(resource=get_dynamodb_resource())
    return __item_repository

def get_recommendation_repository():
    global __recommendation_repository
    if __recommendation_repository is None:
        __recommendation_repository = RecommendationRepository(resource=get_dynamodb_resource())
    return __recommendation_repository

def get_interaction_repository():
    global __interaction_repository
    if __interaction_repository is None:
        __interaction_repository=InteractionTrackingRepository(dynamodb_client=get_dynamodb_client())
    return __interaction_repository

def get_whatsapp_account_repository():
    global __whatsapp_account_repository
    if __whatsapp_account_repository is None:
        __whatsapp_account_repository=WhatsAppAccountRepository(resource=get_dynamodb_resource())
    return __whatsapp_account_repository

def get_whatsapp_job_repository():
    global __whatsapp_job_repository
    if __whatsapp_job_repository is None:
        __whatsapp_job_repository=WhatsAppJobRepository(resource=get_dynamodb_resource())
    return __whatsapp_job_repository

def get_media_file_repository():
    global __media_file_repository
    if __media_file_repository is None:
        __media_file_repository = MediaFileRepository(resource=get_dynamodb_resource())
    return __media_file_repository