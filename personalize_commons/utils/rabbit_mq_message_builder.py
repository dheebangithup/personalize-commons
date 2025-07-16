import json
from datetime import datetime, timezone
from typing import Dict, Any

from personalize_commons.constants.event_type import EventType


class RabbitMqMessageBuilder:

    @staticmethod
    def build_recommendation_message(event_type: EventType,
                                     payload: Dict[str, Any],
                                     source: str,
                                     retry_count: int = 0
                                     ) -> str:
        message = {
            'event_type': str(event_type),
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
            "retry_count": retry_count
        }
        return json.dumps(message)
