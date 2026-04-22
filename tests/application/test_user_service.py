from dataclasses import dataclass, field

import pytest

from app.application.user_service import UserService
from app.domain.events import DomainEvent
from app.domain.user import User


@dataclass
class FakeSession:
    commits: int = 0

    async def commit(self) -> None:
        self.commits += 1


@dataclass
class FakeUserRepository:
    users: list[User] = field(default_factory=list)

    async def add(self, user: User) -> None:
        self.users.append(user)


@dataclass
class FakeOutboxRepository:
    events: list[DomainEvent] = field(default_factory=list)

    async def add(self, event: DomainEvent) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_create_user_persists_user_and_outbox_event() -> None:
    session = FakeSession()
    user_repository = FakeUserRepository()
    outbox_repository = FakeOutboxRepository()
    service = UserService(
        session=session,  # type: ignore[arg-type]
        user_repository=user_repository,  # type: ignore[arg-type]
        outbox_repository=outbox_repository,  # type: ignore[arg-type]
    )

    user = await service.create_user("user@example.com", "Test User")

    assert user_repository.users == [user]
    assert len(outbox_repository.events) == 1
    assert outbox_repository.events[0].event_type == "UserCreated"
    assert outbox_repository.events[0].payload["user_id"] == user.id
    assert session.commits == 1
