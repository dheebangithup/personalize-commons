# context.py
import contextvars

from personalize_commons.constants.app_constants import AppConstants


tenant_id_ctx = contextvars.ContextVar(AppConstants.X_MDM_PERSONALIZE, default=None)
