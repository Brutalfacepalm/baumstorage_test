from datetime import datetime as dt
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.orm.collections import InstrumentedList
from database import Base


class BaseModel(Base):
    """
    It's standard Base model from SQLAlchemy with additional method convert data to Python dict.
    """
    __abstract__ = True

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

    def to_xmessage(self):
        """
        Convert data model to Python dict according to XMessageSchema and return.
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
        data['x_avg_count_in_line'] = data['x_count'] / data['line_count']
        del data['x_count']
        del data['line_count']

        return data


class XText(BaseModel):
    """
    Model for load to database with specially columns.
    datetime - date and time load of source data
    title - title of text data
    x_avg_count_in_line - result job task consumer - average count Х in lines of source data text
    """
    __tablename__ = 'xtext'
    __table_args__ = {'comment': 'Подсчет Х в тексте.'}

    datetime = Column(DateTime(), nullable=False, primary_key=True, comment='Дата и время')
    title = Column(Text(), nullable=False, primary_key=True, comment='Заголовок')
    x_count = Column(Integer(), nullable=False, comment='Среднне число вхождений')
    line_count = Column(Integer(), nullable=False, comment='Среднне число вхождений')
