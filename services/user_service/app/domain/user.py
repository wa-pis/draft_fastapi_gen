from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4


@dataclass
class User:
    id: str
    email: str
    name: str
    created_at: datetime

    @classmethod
    def register(cls, email: str, name: str) -> "User":
        return cls(id=str(uuid4()), email=email, name=name, created_at=datetime.utcnow())

