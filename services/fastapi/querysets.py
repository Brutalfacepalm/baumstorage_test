from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models import XMessages


class XMessageQueryset:
    """
    For work with data and database according to parameters from XMessages model.
    """
    model = XMessages

    @classmethod
    async def create(cls, session, **kwargs):
        """
        Load data to database
        :param session: AsyncSession connect to database
        :param kwargs: data for load
        :return:
        """
        try:
            created = cls.model(**kwargs)
            session.add(created)
            await session.commit()
            await session.flush([created])
        except Exception as e:
            print(e)
            await session.rollback()
        finally:
            await session.close()

    @classmethod
    async def get_stats(cls, session: AsyncSession):
        """
        Select all data from database
        :param session: AsyncSession connect to database
        :return: all data from table
        """
        return await session.scalars(select(cls.model))
