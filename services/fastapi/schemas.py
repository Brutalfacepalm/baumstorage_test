from datetime import datetime as dt
from pydantic import BaseModel, Field, validator


class XMessageSchema(BaseModel):
    """
    Schema for message to select from database and return GET method FastAPI.
    """
    datetime: str = Field(..., description='Дата и время')
    title: str = Field(..., description='Заголовок')
    x_avg_count_in_line: float = Field(..., description='Число вхождений')

    @validator('datetime', pre=True)
    def parse_datetime(cls, value):
        """
        validate and parse field datetime in model of data from database
        """
        return value[:-3]

    @validator('x_avg_count_in_line', pre=True)
    def parse_x_avg_count_in_line(cls, value):
        """
        validate and parse field x_avg_count_in_line in model of data from database
        """
        return round(value, 3)


class XMessageViewSchema(XMessageSchema):
    """
    Additional XMessageSchema where add field ID
    """
    id: int = Field(..., description='ID')


class TextSchema(BaseModel):
    """
    Schema for message load from endpoint FastAPI.
    """
    datetime: dt = Field(..., description='Дата и время')
    title: str = Field(..., description='Заголовок')
    text: str = Field(..., description='Исходный текст')

    @validator('datetime', pre=True)
    def parse_datetime(cls, value):
        """
        validate and parse field datetime in model of data from endpoint of load source data
        """
        return dt.strptime(value, '%d.%m.%Y %H:%M:%S.%f')
