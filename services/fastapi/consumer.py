import asyncio
import os
import json
from aio_pika.message import AbstractIncomingMessage
from aio_pika import ExchangeType, connect_robust
from schemas import TextSchema
from database import engine
from database import sm as session_maker
from querysets import XTextQueryset
from getlogger import get_logger


class SimpleTask:
    """
    SimpleTask. Count Х in message, create correct data and load data to database.
    """
    ngn = engine
    ssn = session_maker
    logger = get_logger('consumer')

    @classmethod
    async def create_one_message(cls, message):
        """
        Calculate average count Х in text and load result to database.
        """
        message = message.dict()
        x_count = message['text'].count('Х')
        async with cls.ssn.begin() as session:

            send_message_db = XTextQueryset()
            await send_message_db.create_or_update(session,
                                                   cls.logger,
                                                   {'datetime': message['datetime'],
                                                    'title': message['title'],
                                                    'x_count': x_count,
                                                    'line_count': 1})

    @classmethod
    async def process_message(cls, message: AbstractIncomingMessage):
        """
        Run task for message.
        :param message: message from queue RabbitMQ
        :return:
        """
        async with message.process():
            message_body = TextSchema.parse_obj(json.loads(message.body.decode()))
            await cls.create_one_message(message_body)


async def consumer_task(routing_key):
    """
    Connect to queue RabbitMQ, get message and run task for load result to database
    :param routing_key: kqy for queue and exchange
    :return:
    """
    simple_task = SimpleTask()
    queue_key = routing_key

    connection = await connect_robust(host=os.environ.get("RABBITMQ_HOST"),
                                      port=int(os.environ.get("RABBITMQ_PORT")),
                                      login=os.environ.get("RABBITMQ_USER"),
                                      password=os.environ.get("RABBITMQ_PASSWORD"))
    channel = await connection.channel(publisher_confirms=False)
    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue(queue_key)
    exchange = await channel.declare_exchange(routing_key,
                                              ExchangeType.X_DELAYED_MESSAGE,
                                              arguments={'x-delayed-type': 'direct'}
                                              )
    await queue.bind(exchange, queue_key)
    await queue.consume(simple_task.process_message)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(consumer_task(os.environ.get('RABBITMQ_QUEUE')))
    loop.run_forever()
