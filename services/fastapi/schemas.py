from pydantic import BaseModel, Field, validator
from datetime import datetime


class XMessageSchema(BaseModel):
    datetime_: datetime = Field(..., description='Дата и время')
    title: str = Field(..., description='Заголовок')
    x_avg_count_in_line: float = Field(..., description='Среднне число вхождений')

    @validator('datetime_', pre=True)
    def parse_datetime(cls, value):
        return datetime.strptime(value, '%d.%m.%Y %H:%M:%S.%f')


class XMessageViewSchema(XMessageSchema):
    id: int = Field(..., description='ID')


class TextSchema(BaseModel):
    datetime_: datetime = Field(..., description='Дата и время')
    title: str = Field(..., description='Заголовок')
    text: str = Field(..., description='Исходный текст')

    @validator('datetime_', pre=True)
    def parse_datetime(cls, value):
        return datetime.strptime(value, '%d.%m.%Y %H:%M:%S.%f')

