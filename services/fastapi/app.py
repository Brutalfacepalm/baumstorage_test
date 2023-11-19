import asyncio
from typing import List

from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from starlette import status

from database import Base, engine
from database import sm as session_maker
from querysets import XMessageQueryset
from schemas import XMessageViewSchema, TextSchema
from rabbit_connection import rabbit_connection

from consumer import task


loop = asyncio.get_event_loop()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await rabbit_connection.connect()
    async with app.state.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    loop.create_task(task())

    yield

    await app.state.engine.dispose()
    await rabbit_connection.disconnect()

app = FastAPI(lifespan=lifespan)
app.state.engine = engine
app.state.session_maker = session_maker


@app.get('/texts', response_model=List[XMessageViewSchema])
async def get_stats_texts(request: Request):
    async with request.app.state.session_maker() as session:
        rows = await XMessageQueryset.get_stats(session)
        if rows:
            return [row.to_dict() for row in rows.all()]
        return [{'datetime_': '01.01.0001 01:01:01.001',
                 'title': 'No data',
                 'x_avg_count_in_line': 0.,
                 'id': 0}]


@app.post('/texts',
          status_code=status.HTTP_200_OK)
async def create_stat_text(data: List[TextSchema]):
    await rabbit_connection.send_message(data, delay=3)


if __name__ == '__main__':
    pass
    # run_app()
