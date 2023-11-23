import os
import json
from aio_pika.robust_connection import AbstractRobustConnection
from aio_pika.robust_channel import AbstractRobustChannel
from aio_pika.robust_exchange import AbstractRobustExchange
from aio_pika import connect_robust
from aio_pika import Message

__all__ = ['RabbitConnection']


class RabbitConnection:
    """
    Work with RabbitMQ queue of messages.
    """
    connection: AbstractRobustConnection | None = None
    channel: AbstractRobustChannel | None = None
    exchange: AbstractRobustExchange | None = None

    async def disconnect(self, logger):
        """
        Disconnect from RabbitMQ and close channel.
        """
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None
        self.channel = None
        logger.info('Disconnect RabbitMQ')

    async def connect(self, logger):
        """
        Connect to RabbitMQ service and create channel and exchange.
        """
        try:
            self.connection = await connect_robust(host=os.environ.get("RABBITMQ_HOST"),
                                                   port=int(os.environ.get("RABBITMQ_PORT")),
                                                   login=os.environ.get("RABBITMQ_USER"),
                                                   password=os.environ.get("RABBITMQ_PASSWORD"))
            self.channel = await self.connection.channel(publisher_confirms=False)
            logger.info('Connect RabbitMQ')
        except Exception as e:
            logger.error(f'Error connection to RabbitMQ with Exception: {e}')
            await self.disconnect(logger)

    async def send_message(self, message, logger, *,
                           exchange, routing_key: str,
                           delay: int = None):
        """
        Publish message in exchange with delay.
        """
        try:
            async with self.channel.transaction():
                headers = None
                if delay:
                    headers = {'x-delay': f'{delay * 1000}'}
                message = message.dict()
                if 'datetime' in message:
                    message['datetime'] = message['datetime'].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
                message = Message(body=json.dumps(message).encode(), headers=headers)
                await exchange.publish(message, routing_key=routing_key, mandatory=False)
        except Exception as e:
            logger.error(f'Error publish message to RabbitMQ with Exception: {e}')
