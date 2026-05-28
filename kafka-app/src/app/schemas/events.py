from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class UserPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    user_id: str = Field(min_length=1)
    email: EmailStr


class IncomingEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: UUID
    event_type: str = Field(min_length=1)
    correlation_id: str | None = None
    payload: UserPayload


class ProcessingResult(BaseModel):
    event_id: UUID
    event_type: str
    status: str
    correlation_id: str | None = None
