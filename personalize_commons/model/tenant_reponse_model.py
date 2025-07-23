from pydantic import Field
from pydantic import BaseModel


class TenantResponseModel(BaseModel):
    tenant_id: str
    status:str
    email:str
    name:str
    uid:str=Field(default='Firebase uid')