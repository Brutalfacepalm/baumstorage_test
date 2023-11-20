from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models import XMessages


class XMessageQueryset:
    """
    For work with data and database according to parameters from XMessages model.
    """
    model = XMessages

    @classmethod
    async def create(cls, session, logger, **kwargs):
        """
        Load data to database
        :param session: AsyncSession connect to database
        :param logger: logger
        :param kwargs: data for load
        """
        try:
            created = cls.model(**kwargs)
            session.add(created)
            await session.commit()
            await session.flush([created])
            logger.info('Correct done SimpleTask and load to database')
        except Exception as e:
            logger.error(f'Error load data to database with exception {e}')
            logger.info('Session rollback')
            await session.rollback()
        finally:
            await session.close()
            logger.info('Session close')

    @classmethod
    async def get_stats(cls, session: AsyncSession, logger):
        """
        Select all data from database
        :param session: AsyncSession connect to database
        :param logger: logger
        :return: all data from table
        """
        stats = await session.scalars(select(cls.model))
        logger.info('Select from database correct')
        return stats
