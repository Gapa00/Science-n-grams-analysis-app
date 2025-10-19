from pydantic import BaseModel, StringConstraints
from typing import Annotated

class LoginRequest(BaseModel):
    username: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]

class LoginResponse(BaseModel):
    user_id: int
    username: str
