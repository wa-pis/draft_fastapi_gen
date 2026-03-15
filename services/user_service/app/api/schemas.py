from datetime import datetime

from pydantic import BaseModel, EmailStr


class UserCreateRequest(BaseModel):
    email: EmailStr
    name: str


class UserResponse(BaseModel):
    id: str
    email: EmailStr
    name: str
    created_at: datetime

