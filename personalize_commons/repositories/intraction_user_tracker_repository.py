import os
import time
from personalize_commons.utils.datetime_utils import ist_now
from personalize_commons.entity.intraction_user_tracker_entity import IntractionUserTrackerEntity


class InteractionUserTrackerRepository:
    """
    Repository for the intraction_user_tracker table.
    Schema:
      - PK: tenant_month (S)   -> f"{tenant_id}#{YYYY-MM}"
      - SK: user_id (S)
      - TTL: expire_at (N)     -> optional (enable TTL in table settings)
    """

    def __init__(self, client):
        self.dynamodb = client
        self.table_name = os.getenv("INTERACTION_USER_TRACKER_TABLE", "interaction_user_tracker")
        self.ttl_days = int(os.getenv("INTRACTION_USER_TRACKER_TTL_DAYS", "730"))  # default ~24 months

    def _compute_ttl_epoch(self) -> int:
        """Compute TTL expiry as epoch seconds from now."""
        now_epoch = int(time.time())
        return now_epoch + self.ttl_days * 86400

    def mark_user_seen_once(self, entity: IntractionUserTrackerEntity) -> bool:
        """
        Try to insert a (tenant_id, month, user_id) record.
        Returns:
          True  -> if user is new for this month
          False -> if user already exists (no new insert)
        """
        try:
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item={
                    "tenant_month": {"S": entity.tenant_month},
                    "user_id": {"S": entity.user_id},
                    "expire_at": {"N": str(entity.expire_at or self._compute_ttl_epoch())},
                },
                ConditionExpression="attribute_not_exists(user_id)",
            )
            return True
        except self.dynamodb.exceptions.ConditionalCheckFailedException:
            return False

    def get_user(self, tenant_id: str, user_id: str, month: str = None) -> IntractionUserTrackerEntity | None:
        """
        Fetch a specific user tracker entry.
        Returns Pydantic entity or None if not found.
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        tenant_month = f"{tenant_id}#{month}"
        response = self.dynamodb.get_item(
            TableName=self.table_name,
            Key={"tenant_month": {"S": tenant_month}, "user_id": {"S": user_id}},
        )
        item = response.get("Item")
        if not item:
            return None

        return IntractionUserTrackerEntity(
            tenant_id=tenant_id,
            month=month,
            user_id=user_id,
            expire_at=int(item.get("expire_at", {}).get("N", "0")) if "expire_at" in item else None,
        )
