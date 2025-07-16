class RabbitMQConstants:
    RECOMMENDATION_EXCHANGE  = 'personalize'
    DLQ_EXCHANGE = "recommendations.dlx"
    RECOMMENDATION_QUEUE = 'recommendations'
    RECOMMENDATION_DLQ = "recommendations.dlq"
    RECOMMENDATION_START_ROUTING_KEY = 'recommendation.start'
    RECOMMENDATION_DLQ_ROUTING_KEY = "recommendation.dlq"

    class Payload:
        CAMPAIGN_ID='campaign_id'
        RECOMMENDATION_ID='recommendation_id'

