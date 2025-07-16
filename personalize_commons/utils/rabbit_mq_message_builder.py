import json
from datetime import datetime, timezone
from typing import Dict, Any

from personalize_commons.constants.event_type import EventType
from personalize_commons.constants.rabbit_mq_constants import RabbitMQConstants


class RabbitMqMessageBuilder:

    @staticmethod
    def build_recommendation_message(event_type: EventType,
                                     payload: Dict[str, Any],
                                     source: str,
                                     retry_count: int = 0
                                     ) -> str:
        message = {
            RabbitMQConstants.Payload.EVENT_TYPE: str(event_type),
            RabbitMQConstants.Payload.SOURCE:source,
            RabbitMQConstants.Payload.TIMESTAMP: datetime.now(timezone.utc).isoformat(),
            RabbitMQConstants.Payload.PAYLOAD: payload,
            RabbitMQConstants.Payload.RETRY_COUNT: retry_count
        }
        return json.dumps(message)
