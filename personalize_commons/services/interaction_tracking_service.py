from typing import Dict, Optional

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.entity.intraction_tracking_entity import InteractionTrackingEntity
from personalize_commons.repositories.intraction_entity_tracking_repository import InteractionTrackingRepository
from personalize_commons.repositories.intraction_user_tracker_repository import InteractionUserTrackerRepository
from personalize_commons.utils.datetime_utils import ist_now

# Entities
from personalize_commons.entity.intraction_user_tracker_entity import IntractionUserTrackerEntity


class InteractionTrackingService:
    """
    High-level service that:
      - Tracks aggregate interaction counts
      - Tracks distinct end-users per (tenant, month)
    """

    def __init__(
        self,
        tracking_repo: InteractionTrackingRepository,
        user_tracker_repo: InteractionUserTrackerRepository,
    ):
        self.tracking_repo = tracking_repo
        self.user_tracker_repo = user_tracker_repo

    def track_interaction(
        self,
        tenant_id: str,
        user_id: str,
        event_increments: Optional[Dict[str, int]] = None,
        month: Optional[str] = None,
    ) -> dict:
        """
        1) Ensures unique user counting (once per tenant/month).
        2) Increments aggregate interaction counters.

        Returns dict with metadata about what was updated.
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        result = {
            AppConstants.TENANT_ID: tenant_id,
            DBConstants.MONTH: month,
            "unique_user_incremented": False,
            "interactions_updated": {},
        }

        # Step 1: Build tracker entity and attempt insert
        tracker_entity = IntractionUserTrackerEntity(
            tenant_id=tenant_id,
            user_id=user_id,
            month=month,
        )
        is_first_time = self.user_tracker_repo.mark_user_seen_once(tracker_entity)
        if is_first_time:
            self.tracking_repo.increment_unique_users(tenant_id, month, by=1)
            result["unique_user_incremented"] = True

        # Step 2: Update interaction counters (optional)
        if event_increments:
            updated = self.tracking_repo.update_interactions(tenant_id, event_increments, month)
            result["interactions_updated"] = updated

        return result

    def get_monthly_summary(
        self, tenant_id: str, month: Optional[str] = None
    ) -> InteractionTrackingEntity:
        """
        Returns aggregated monthly summary for tenant.
        """
        return self.tracking_repo.get_interactions(tenant_id, month)

''' USAGE EXAMPLE
# Track interaction
result = service.track_interaction(
    tenant_id="tenant123",
    user_id="user567",
    event_increments={"purchase": 3, "add_to_cart": 1},
)
print(result)
# {
#   "tenant_id": "tenant123",
#   "month": "2025-08",
#   "unique_user_incremented": True,
#   "interactions_updated": {...}
# }

# Get monthly summary
summary = service.get_monthly_summary("tenant123")
print(summary.dict())
# {
#   "tenant_id": "tenant123",
#   "month": "2025-08",
#   "interactions": {"purchase": 3, "add_to_cart": 1},
#   "unique_users": 1
# }

'''