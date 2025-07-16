from enum import Enum

class EventType(str, Enum):
    RECOMMENDATION_TRIGGERED = "recommendation.triggered"
    RECOMMENDATION_COMPLETED = "recommendation.completed"
    RECOMMENDATION_FAILED = "recommendation.failed"

    NOTIFICATION_TRIGGERED = "notification.triggered"
    NOTIFICATION_COMPLETED = "notification.completed"
    NOTIFICATION_FAILED = "notification.failed"


