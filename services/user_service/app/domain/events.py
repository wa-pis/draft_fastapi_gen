from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any
from uuid import uuid4


@dataclass
class DomainEvent:
    id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: datetime
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def new_event(event_type: str, aggregate_type: str, aggregate_id: str, payload: dict[str, Any]) -> DomainEvent:
    return DomainEvent(
        id=str(uuid4()),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=datetime.utcnow(),
        payload=payload,
    )


def user_created(user_id: str, email: str, name: str) -> DomainEvent:
    return new_event(
        event_type="UserCreated",
        aggregate_type="User",
        aggregate_id=user_id,
        payload={"user_id": user_id, "email": email, "name": name},
    )

