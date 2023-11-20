import json
from aio_pika.message import AbstractIncomingMessage
from aio_pika import ExchangeType
import aio_pika
from querysets import XMessageQueryset
from schemas import TextSchema


class SimpleTask:
    """
    SimpleTask. Count Х in message, create correct data and load data to database.
    """
    session = None
    logger = None

    @classmethod
    async def simple_task(cls, dt, title, text):
        """
        Calculate average count Х in text and load result to database.
        """
        x_count = 0
        messages = text.split('\n')
        m_count = len(messages)
        for message in messages:
            x_count += message.count('Х')
        async with cls.session.begin() as session:
            send_message_db = XMessageQueryset()
            await send_message_db.create(session,
                                         cls.logger, **{'datetime': dt,
                                                        'title': title,
                                                        'x_avg_count_in_line': x_count / m_count})


async def process_message(message: AbstractIncomingMessage):
    """
    Run task for message.
    :param message: message from queue RabbitMQ
    :return:
    """
    async with message.process():
        message = TextSchema.parse_obj(json.loads(message.body.decode()))
        method = SimpleTask.simple_task
        if method:
            await method(message.datetime, message.title, message.text)


async def task(session, logger):
    """
    Connect to queue RabbitMQ, get message and run task for load result to database
    :param session: AsyncSession sqlalchemy to database
    :param logger: logger
    :return:
    """
    SimpleTask.session = session
    SimpleTask.logger = logger
    queue_key = 'main'
    url_queue = 'amqp://rmuser:rmpassword@rabbitmq:5672/'

    connection = await aio_pika.connect_robust(url_queue)
    channel = await connection.channel(publisher_confirms=False)
    await channel.set_qos(prefetch_count=100)
    queue = await channel.declare_queue(queue_key)
    exchange = await channel.declare_exchange('main',
                                              ExchangeType.X_DELAYED_MESSAGE,
                                              arguments={'x-delayed-type': 'direct'}
                                              )
    await queue.bind(exchange, queue_key)
    await queue.consume(process_message)
