from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from models import XText


class XTextQueryset:
    """
    For work with data and database according to parameters from XMessages model.
    """
    model = XText

    @classmethod
    async def create_or_update(cls, session, logger, data_dict):
        """
        Load data to database
        :param session: AsyncSession connect to database
        :param logger: logger
        :param data_dict: data for load
        """
        try:
            to_update = await session.scalar(select(cls.model).where((cls.model.datetime == data_dict['datetime']) &
                                                                     (cls.model.title == data_dict['title'])))
            if to_update:
                to_update = to_update.to_dict()
                data_dict['x_count'] += to_update['x_count']
                data_dict['line_count'] += to_update['line_count']
                await session.execute(update(cls.model).where((cls.model.datetime == data_dict['datetime']) &
                                                              (cls.model.title == data_dict['title'])).values(
                    {'x_count': data_dict['x_count'], 'line_count': data_dict['line_count']}))
                await session.commit()
            else:
                created = cls.model(**data_dict)
                session.add(created)
                await session.commit()
                await session.flush([created])
            logger.info(f'Update data {data_dict["datatime"]}:{data_dict["title"]} on table xtext success.')
        except Exception as e:
            logger.error(f'Create or Update xtext was complete with error: {e}')
            await session.rollback()
        finally:
            await session.close()

    @classmethod
    async def get_stats(cls, session: AsyncSession, logger):
        """
        Select all data from database
        :param session: AsyncSession connect to database
        :param logger: logger
        :return: all data from table
        """
        stats = await session.scalars(select(cls.model).order_by(cls.model.datetime))
        logger.info('Select from database correct')
        return stats
