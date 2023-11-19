from aio_pika.robust_connection import AbstractRobustConnection
from aio_pika.robust_channel import AbstractRobustChannel
from aio_pika.robust_exchange import AbstractRobustExchange
from aio_pika import connect_robust, ExchangeType
from aio_pika import Message
import json

__all__ = ['rabbit_connection']


class RabbitConnection:
    _connection: AbstractRobustConnection | None = None
    _channel: AbstractRobustChannel | None = None
    _exchange: AbstractRobustExchange | None = None

    async def disconnect(self) -> None:
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None

    async def connect(self) -> None:
        try:
            url_broker = 'amqp://rmuser:rmpassword@rabbitmq:5672/'
            self._connection = await connect_robust(url_broker)
            self._channel = await self._connection.channel(publisher_confirms=False)
            self._exchange = await self._channel.declare_exchange('main',
                                                                  ExchangeType.X_DELAYED_MESSAGE,
                                                                  arguments={'x-delayed-type': 'direct'}
                                                                  )
        except Exception as e:
            print(e)
            await self.disconnect()

    async def send_message(self, messages, *, routing_key: str = 'main', delay: int = None):
        try:
            async with self._channel.transaction():
                headers = None
                if delay:
                    headers = {'x-delay': f'{delay * 1000}'}
                for message in messages:
                    message = message.dict()
                    if 'datetime_' in message:
                        message['datetime_'] = message['datetime_'].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
                    message = Message(body=json.dumps(message).encode(), headers=headers)
                    await self._exchange.publish(message, routing_key=routing_key, mandatory=False if delay else True)
                    print('DONE!')
        except Exception as e:
            print(e)


rabbit_connection = RabbitConnection()
