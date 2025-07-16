class RabbitMQConstants:
    EXCHANGE_NAME = 'personalize'
    RECOMMENDATION_QUEUE = 'recommendation'
    RECOMMENDATION_DLQ = "recommendations.dlq"
    RECOMMENDATION_START_ROUTING_KEY = 'personalize.start'
    RECOMMENDATION_DLQ__ROUTING_KEY = "recommendation.dlq"

