from aio_pika.message import AbstractIncomingMessage
from aio_pika import ExchangeType
import aio_pika
import asyncio
from task import simple_task
import json
from schemas import TextSchema


async def process_message(message: AbstractIncomingMessage):
    async with message.process():
        message = TextSchema.parse_obj(json.loads(message.body.decode()))
        method = simple_task
        if method:
            await method(message.datetime_, message.title, message.text)


async def task():
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
    try:
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(task())
