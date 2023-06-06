from pydantic import BaseModel


class AccessRequest(BaseModel):
    client_id: int
    clinic_id: int
