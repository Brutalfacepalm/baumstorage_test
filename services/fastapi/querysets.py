from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import XMessages


class XMessageQueryset:
    model = XMessages

    @classmethod
    async def create(cls, session, **kwargs):
        created = cls.model(**kwargs)
        session.add(created)
        await session.flush([created])
        return created.id

    @classmethod
    async def get_stats_by_id(cls, session: AsyncSession, id_):
        return await session.scalar(select(cls.model).where(cls.model.id == id_))

    @classmethod
    async def get_stats(cls, session: AsyncSession):
        return await session.scalar(select(cls.model))

