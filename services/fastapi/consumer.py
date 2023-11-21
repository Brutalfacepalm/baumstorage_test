import os
import json
from aio_pika.message import AbstractIncomingMessage
from aio_pika import ExchangeType, connect_robust
from schemas import TextSchema


class SimpleTask:
    """
    SimpleTask. Count Х in message, create correct data and load data to database.
    """
    def __int__(self):
        self.x_count = 0
        self.line_count = 0

    async def simple_task(self, text):
        """
        Calculate average count Х in text and load result to database.
        """
        self.x_count += text.count('Х')
        self.line_count += 1

    async def process_message(self, message: AbstractIncomingMessage):
        """
        Run task for message.
        :param message: message from queue RabbitMQ
        :return:
        """
        async with message.process():
            message = TextSchema.parse_obj(json.loads(message.body.decode()))
            method = self.simple_task
            if method:
                await method(message.text)


async def task(routing_key):
    """
    Connect to queue RabbitMQ, get message and run task for load result to database
    :param routing_key: kqy for queue and exchange
    :return:
    """
    simple_task = SimpleTask()
    simple_task.x_count = 0
    simple_task.line_count = 0
    queue_key = routing_key

    connection = await connect_robust(host=os.environ.get("RABBITMQ_HOST"),
                                      port=int(os.environ.get("RABBITMQ_PORT")),
                                      login=os.environ.get("RABBITMQ_USER"),
                                      password=os.environ.get("RABBITMQ_PASSWORD"))
    channel = await connection.channel(publisher_confirms=False)
    await channel.set_qos()
    queue = await channel.declare_queue(queue_key, auto_delete=True, timeout=10000)
    exchange = await channel.declare_exchange(queue_key,
                                              ExchangeType.X_DELAYED_MESSAGE,
                                              arguments={'x-delayed-type': 'direct'}
                                              )
    await queue.bind(exchange, queue_key)
    await queue.consume(simple_task.process_message)

    return simple_task, channel, queue



