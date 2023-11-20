from datetime import datetime as dt
from sqlalchemy import Column, Float, Integer, Text, DateTime
from sqlalchemy.orm.collections import InstrumentedList
from database import Base


class BaseModel(Base):
    """
    It's standard Base model from SQLAlchemy with additional method convert data to Python dict.
    """
    __abstract__ = True
    id = Column(Integer, primary_key=True, comment='ID')

    def to_dict(self):
        """
        Convert data model to Python dict and return.
        """
        data = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, Base):
                v = v.to_dict()
            elif isinstance(v, InstrumentedList):
                v = [item.to_dict() for item in v]
            elif isinstance(v, dt):
                v = v.strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]
            data[k] = v
        return data


class XMessages(BaseModel):
    """
    Model for load to database with specially columns.
    datetime - date and time load of source data
    title - title of text data
    x_avg_count_in_line - result job task consumer - average count Х in lines of source data text
    """
    __tablename__ = 'xmessages'
    __table_args__ = {'comment': 'Подсчет Х в сообщениях.'}

    datetime = Column(DateTime(), nullable=False, comment='Дата и время')
    title = Column(Text(), nullable=False, comment='Заголовок')
    x_avg_count_in_line = Column(Float(), nullable=False, comment='Среднне число вхождений')
