from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User


class DuplicateUserEmailError(ValueError):
    pass


@dataclass(kw_only=True, frozen=True, slots=True)
class UserRepository:
    session: AsyncSession

    async def add(self, user: User) -> None:
        try:
            await self.session.execute(
                text(
                    """
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, :created_at)
                    """
                ),
                {
                    "id": user.id,
                    "email": user.email,
                    "name": user.name,
                    "created_at": user.created_at,
                },
            )
        except IntegrityError as exc:
            raise DuplicateUserEmailError(user.email) from exc
