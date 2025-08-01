from personalize_commons.constants.app_constants import AppConstants
from recombee_api_client.api_client import RecombeeClient, Region

recombee_api_client = None

intraction_service = None


def get_recombee_client(tenant: dict[str, str]) -> RecombeeClient:
    global recombee_api_client
    if recombee_api_client is None:
        recombee_api_client = RecombeeClient(
            database_id=tenant.get(AppConstants.TENANT_DATA_BASE_ID),
            token=tenant.get(AppConstants.TENANT_PRIVATE_KEY),
            region=get_region(tenant.get(AppConstants.TENANT_REGION)),
        )

    return recombee_api_client


def get_region(region_name: str) -> Region | None:
    match region_name:
        case Region.AP_SE.name:
            return Region.AP_SE
        case Region.CA_EAST.name:
            return Region.CA_EAST
        case Region.EU_WEST.name:
            return Region.EU_WEST
        case Region.US_WEST.name:
            return Region.US_WEST
    return None
