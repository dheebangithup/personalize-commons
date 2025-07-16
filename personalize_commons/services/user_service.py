import logging
from typing import Dict, List, Any, Optional

from personalize_commons.model.user_model import QueryResponse
from personalize_commons.repositories.user_repository import UserRepository


logger = logging.getLogger(__name__)


class UserService:
    def __init__(self, user_repo: UserRepository):
        self.user_repo = user_repo
        logger.info("Initialized UserService")

    def query_users(self, conditions: Dict[str, Any],tenant_id:str) -> QueryResponse:
        """
        Query users with flexible conditions using the repository.
        Returns a QueryResponse containing items and count
        """
        try:
            return self.user_repo.query_users(conditions,tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Error querying users: {str(e)}", exc_info=True)
            raise
