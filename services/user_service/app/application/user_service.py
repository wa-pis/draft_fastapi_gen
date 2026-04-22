from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.events import user_created
from app.domain.user import User
from app.infra.outbox import OutboxRepository
from app.infra.repositories import UserRepository


@dataclass(kw_only=True, frozen=True, slots=True)
class UserService:
    user_repository: UserRepository
    outbox_repository: OutboxRepository
    session: AsyncSession

    async def create_user(self, email: str, name: str) -> User:
        user = User.register(email=email, name=name)
        await self.user_repository.add(user)
        event = user_created(user.id, user.email, user.name)
        await self.outbox_repository.add(event)
        await self.session.commit()
        return user
