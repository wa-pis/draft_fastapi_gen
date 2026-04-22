from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User


class DuplicateUserEmailError(ValueError):
    pass


def is_duplicate_email_error(exc: IntegrityError) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return (
        "duplicate key" in message
        and "unique constraint" in message
        and ("idx_users_email" in message or "users_email" in message or "email" in message)
    )


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
            if not is_duplicate_email_error(exc):
                raise
            raise DuplicateUserEmailError(user.email) from exc
