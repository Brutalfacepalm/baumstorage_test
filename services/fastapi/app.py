import asyncio
from typing import List
from contextlib import asynccontextmanager
from starlette import status

from fastapi import FastAPI, Request

from database import Base, engine
from database import sm as session_maker
from querysets import XMessageQueryset
from schemas import MessageSchema, TextSchema
from rabbit_connection import RabbitConnection

from consumer import task

loop = asyncio.get_event_loop()


# TO DO LOGGER


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Lifespan function.
    When main app is startup it's connect to rabbitmq, database and run async loop task consumer.
    When main app is shutdown it's disconnect rabbitmq, database and close async loop.
    """
    await app.rabbit_connection.connect()
    async with app.state.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        loop.create_task(task(app.state.session_maker))

    yield

    await app.state.engine.dispose()
    await app.rabbit_connection.disconnect()
    loop.close()


app = FastAPI(lifespan=lifespan)
app.state.engine = engine
app.state.session_maker = session_maker
app.rabbit_connection = RabbitConnection()


@app.get('/texts', response_model=List[MessageSchema])
async def get_stat_texts(request: Request):
    """
    Method GET.
    Connect to database and return all data in table.
    Data from database validate according to class MessageSchema.
    """
    async with request.app.state.session_maker() as session:
        rows = await XMessageQueryset.get_stats(session)
        if rows:
            return [row.to_dict() for row in rows.all()]
        return [{'datetime': '01.01.0001 00:00:00.000',
                 'title': 'No data',
                 'x_avg_count_in_line': 0.}]


@app.post('/texts',
          status_code=status.HTTP_200_OK)
async def load_and_send_text(data: TextSchema):
    """
     Method POST.
     Need for load source data and publish to messages queue RabbitMQ.
     Data validate according to TextSchema.
    """
    await app.rabbit_connection.send_message(messages=data, delay=3)


if __name__ == '__main__':
    pass
