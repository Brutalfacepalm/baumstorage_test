import pytest
from httpx import AsyncClient
from app import app


@pytest.mark.anyio
async def test_connect_to_api():
    async with AsyncClient(app=app, base_url='http://test') as ac:
        response = await ac.get('/')
    assert response.status_code == 200
