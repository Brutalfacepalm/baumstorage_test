import os
import json
from aio_pika.robust_connection import AbstractRobustConnection
from aio_pika.robust_channel import AbstractRobustChannel
from aio_pika.robust_exchange import AbstractRobustExchange
from aio_pika import connect_robust, ExchangeType
from aio_pika import Message

__all__ = ['RabbitConnection']


class RabbitConnection:
    """
    Work with RabbitMQ queue of messages.
    """
    _connection: AbstractRobustConnection | None = None
    _channel: AbstractRobustChannel | None = None
    _exchange: AbstractRobustExchange | None = None

    async def disconnect(self, logger):
        """
        Disconnect from RabbitMQ and close channel.
        """
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        logger.info('Disconnect RabbitMQ')

    async def connect(self, logger):
        """
        Connect to RabbitMQ service and create channel and exchange.
        """
        try:
            self._connection = await connect_robust(host=os.environ.get("RABBITMQ_HOST"),
                                                    port=int(os.environ.get("RABBITMQ_PORT")),
                                                    login=os.environ.get("RABBITMQ_USER"),
                                                    password=os.environ.get("RABBITMQ_PASSWORD"))
            self._channel = await self._connection.channel(publisher_confirms=False)
            self._exchange = await self._channel.declare_exchange('main',
                                                                  ExchangeType.X_DELAYED_MESSAGE,
                                                                  arguments={'x-delayed-type': 'direct'}
                                                                  )
            logger.info('Connect RabbitMQ')
        except Exception as e:
            logger.error(f'Error connection to RabbitMQ with Exception: {e}')
            await self.disconnect(logger)

    async def send_message(self, message, logger, *, routing_key: str = 'main', delay: int = None):
        """
        Publish message in exchange with delay.
        """
        try:
            async with self._channel.transaction():
                headers = None
                if delay:
                    headers = {'x-delay': f'{delay * 1000}'}
                # for message in [messages]:
                message = message.dict()
                if 'datetime' in message:
                    message['datetime'] = message['datetime'].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
                message = Message(body=json.dumps(message).encode(), headers=headers)
                await self._exchange.publish(message, routing_key=routing_key, mandatory=False if delay else True)
                logger.info('Correct publish message to RabbitMQ')
        except Exception as e:
            logger.error(f'Error publish message to RabbitMQ with Exception: {e}')
