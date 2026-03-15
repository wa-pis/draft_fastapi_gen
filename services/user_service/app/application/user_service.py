from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.events import user_created
from app.infra.outbox import add_outbox_event


class UserService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_user(self, email: str, name: str) -> User:
        user = User.register(email=email, name=name)
        await self._session.execute(
            "INSERT INTO users (id, email, name, created_at) VALUES (:id, :email, :name, :created_at)",
            {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "created_at": user.created_at,
            },
        )
        event = user_created(user.id, user.email, user.name)
        await add_outbox_event(self._session, event)
        await self._session.commit()
        return user

