import os
import asyncio
from typing import List
from contextlib import asynccontextmanager
from starlette import status
from fastapi import FastAPI, Request
from aio_pika import ExchangeType

from database import Base, engine
from database import sm as session_maker
from querysets import XMessageQueryset
from schemas import XMessageSchema, TextSchema
from rabbit_connection import RabbitConnection
from consumer import consumer_task
from getlogger import get_logger


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Lifespan function.
    When main app is startup it's connect to rabbitmq, database and run async loop task consumer.
    When main app is shutdown it's disconnect rabbitmq, database and close async loop.
    """
    await app.rabbit_connection.connect(logger)
    async with app.state.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        logger.info('Startup FastAPI')
    yield

    await app.state.engine.dispose()
    await app.rabbit_connection.disconnect(logger)
    logger.info('Shutdown FastAPI')

logger = get_logger()
app = FastAPI(lifespan=lifespan, docs_url='/',
              debug=True if os.environ.get('FASTAPI_DEBUG') == 'on' else False)
app.state.engine = engine
app.state.session_maker = session_maker
app.rabbit_connection = RabbitConnection()
logger.info('Application FastAPI was created')


@app.get('/get_stats', response_model=List[XMessageSchema])
async def get_stat_texts(request: Request):
    """
    Method GET.
    Connect to database and return all data in table.
    Data from database validate according to class MessageSchema.
    """
    async with request.app.state.session_maker() as session:
        rows = await XMessageQueryset.get_stats(session, logger)
        if rows:
            return [row.to_xmessage() for row in rows.all()]
        return [{'datetime': '01.01.0001 00:00:00.000',
                 'title': 'No data',
                 'x_avg_count_in_line': 0.}]


async def load_to_database(dt, title, result):
    async with app.state.session_maker.begin() as session:
        send_message_db = XMessageQueryset()
        await send_message_db.merge(session,
                                    logger,
                                    {'datetime': dt,
                                     'title': title,
                                     'x_count': result.x_count,
                                     'line_count': result.line_count})
        logger.info('All stats load to database')


async def send_message(dt, title, line, exchange, declare_exchange_key):
    message = TextSchema(**{'datetime': dt, 'title': title, 'text': line})
    await app.rabbit_connection.send_message(message=message, routing_key=declare_exchange_key,
                                             exchange=exchange, logger=logger, delay=0)
    await asyncio.sleep(0.01)


@app.post('/send_text', status_code=status.HTTP_200_OK)
async def load_and_send_text(data: TextSchema):
    """
     Method POST.
     Need for load source data and publish to messages queue RabbitMQ.
     Data validate according to TextSchema.
    """
    logger.info('Get new source data')
    source_data = data.dict()
    dt = source_data['datetime'].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
    title = source_data['title']
    declare_exchange_key = str(source_data['datetime'].timestamp()) + title

    exchange = await app.rabbit_connection._channel.declare_exchange(declare_exchange_key,
                                                                     ExchangeType.X_DELAYED_MESSAGE,
                                                                     arguments={'x-delayed-type': 'direct'}
                                                                     )
    logger.info('Run async task')
    result, channel, queue = await consumer_task(declare_exchange_key)
    try:
        logger.info('Start send messages after split text')
        for line in source_data['text'].split('\n'):
            line = line.strip()
            if line:
                await send_message(dt, title, line, exchange, declare_exchange_key)
        logger.info('All messages correct publish to RabbitMQ')
    except Exception as e:
        print(e)
        logger.info(f'Error send any message to RabbitMQ with {e}')

    await load_to_database(source_data['datetime'], title, result)
    await channel.exchange_delete(declare_exchange_key)
    logger.info('Current exchange was delete')


if __name__ == '__main__':
    pass
