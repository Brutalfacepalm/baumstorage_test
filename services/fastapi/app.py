import os
import asyncio

import click
import uvicorn
from typing import List
from starlette import status
from aio_pika import ExchangeType
from fastapi import FastAPI, Request

from database import Base, engine
from database import sm as session_maker
from querysets import XTextQueryset
from schemas import XTextSchema, TextSchema
from rabbit_connection import RabbitConnection
from getlogger import get_logger


def create_app():
    """
    Create and return application FastAPI
    :return: created application
    """
    logger = get_logger()
    app = FastAPI(docs_url='/', debug=True if os.environ.get('FASTAPI_DEBUG') == 'on' else False)
    app.state.engine = engine
    app.state.session_maker = session_maker
    app.rabbit_connection = RabbitConnection()
    logger.info('Application FastAPI was created')

    @app.on_event("startup")
    async def startup_event():
        """
        When main app is startup it's connect to rabbitmq, database and run async loop task consumer.
        """
        await app.rabbit_connection.connect(logger)
        async with app.state.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logger.info('Startup FastAPI')

    @app.on_event("shutdown")
    async def on_exit_app():
        """
        When main app is shutdown it's disconnect rabbitmq, database and close async loop.
        """
        await app.state.engine.dispose()
        await app.rabbit_connection.disconnect(logger)
        logger.info('Shutdown FastAPI')

    @app.get('/get_stats', response_model=List[XTextSchema])
    async def get_stat_texts(request: Request):
        """
        Method GET.
        Connect to database and return all data in table.
        Data from database validate according to class MessageSchema.
        """
        async with request.app.state.session_maker() as session:
            rows = await XTextQueryset.get_stats(session, logger)
            if rows:
                return [row.to_xmessage() for row in rows.all()]
            return [{'datetime': '01.01.0001 00:00:00.000',
                     'title': 'No data',
                     'x_avg_count_in_line': 0.}]

    @app.post('/send_text', status_code=status.HTTP_200_OK)
    async def load_and_send_text(data: TextSchema):
        """
         Method POST.
         Need for load source data and publish to messages queue RabbitMQ.
         Data validate according to TextSchema.
        """
        logger.info('Get new source data')
        source_data = data.dict()
        exchange = await app.rabbit_connection.channel.declare_exchange(os.environ.get('RABBITMQ_QUEUE'),
                                                                        ExchangeType.X_DELAYED_MESSAGE,
                                                                        arguments={'x-delayed-type': 'direct'}
                                                                        )
        dt = source_data['datetime'].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
        title = source_data['title']
        try:
            logger.info('Start send messages after split text')
            for line in source_data['text'].split('\n'):
                message = TextSchema(**{'datetime': dt,
                                        'title': title,
                                        'text': line})
                await app.rabbit_connection.send_message(message=message,
                                                         routing_key=os.environ.get('RABBITMQ_QUEUE'),
                                                         exchange=exchange,
                                                         logger=logger,
                                                         delay=0)
                await asyncio.sleep(3)
            logger.info('All messages correct publish to RabbitMQ')
        except Exception as e:
            logger.info(f'Error send any message to RabbitMQ with {e}')

    return app


@click.command()
@click.option('--host', '-h', default='0.0.0.0')
@click.option('--port', '-p', default=8888)
@click.option('--workers', '-p', default=1)
def main(host, port, workers):
    """
    Run application FastAPI
    :param host: host address, default 0.0.0.0
    :param port: port number, default 8888
    :param workers: number of workers, default 1
    :return:
    """
    uvicorn.run(f'{__name__}:create_app',
                host=host, port=port,
                workers=workers)


if __name__ == '__main__':
    asyncio.run(main())
