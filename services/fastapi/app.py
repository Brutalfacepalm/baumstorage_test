import os
import asyncio
from typing import List
from contextlib import asynccontextmanager
from starlette import status
from fastapi import FastAPI, Request

from database import Base, engine
from database import sm as session_maker
from querysets import XMessageQueryset
from schemas import XMessageSchema, TextSchema
from rabbit_connection import RabbitConnection
from consumer import task
from getlogger import get_logger


loop = asyncio.get_event_loop()


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
        loop.create_task(task(app.state.session_maker, logger))
        logger.info('Startup FastAPI')
    yield

    await app.state.engine.dispose()
    await app.rabbit_connection.disconnect(logger)
    loop.close()
    logger.info('Shutdown FastAPI')

logger = get_logger()
app = FastAPI(lifespan=lifespan, docs_url='/',
              debug=True if os.environ.get('FASTAPI_DEBUG') == 'on' else False)
app.state.engine = engine
app.state.session_maker = session_maker
app.rabbit_connection = RabbitConnection()
logger.info('Application FastAPI was created')


@app.get('/texts', response_model=List[XMessageSchema])
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
                 'x_count': 0,
                 'line_count': 0}]


@app.post('/texts',
          status_code=status.HTTP_200_OK)
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
    logger.info('Start send messages after split text')
    for line in source_data['text'].split('\n'):
        message = TextSchema(**{'datetime': dt,
                                'title': title,
                                'text': line})
        await asyncio.sleep(3)
        await app.rabbit_connection.send_message(message=message, logger=logger, delay=0)


if __name__ == '__main__':
    pass
