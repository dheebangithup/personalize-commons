# src/dependencies/repositories.py

from personalize_commons.repositories.campaign_repository import CampaignRepository
from personalize_commons.repositories.intraction_entity_tracking_repository import InteractionTrackingRepository
from personalize_commons.repositories.intraction_user_tracker_repository import InteractionUserTrackerRepository

from personalize_commons.repositories.item_repository import ItemRepository
from personalize_commons.repositories.recommendation_repository import RecommendationRepository
from personalize_commons.repositories.tenant_repository import TenantRepository
from personalize_commons.repositories.user_repository import UserRepository

from personalize_commons.dependencies.aws_providers import get_dynamodb_resource, get_dynamodb_client

# Create singleton instances
__user_repository = None
__campaign_repository = None
__item_repository = None
__recommendation_repository = None
__tenant_repository = None
__intraction_tracking_repository = None
__interaction_user_tracking_repository = None


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

def get_intraction_tracking_repository()->InteractionTrackingRepository:
    global __intraction_tracking_repository
    if __intraction_tracking_repository is None:
        __intraction_tracking_repository=InteractionTrackingRepository(client=get_dynamodb_client())
    return __intraction_tracking_repository

def get_interaction_user_tracking_repository():
    global __interaction_user_tracking_repository
    if __interaction_user_tracking_repository is None:
        __interaction_user_tracking_repository=InteractionUserTrackerRepository(client=get_dynamodb_client())
    return __interaction_user_tracking_repository
