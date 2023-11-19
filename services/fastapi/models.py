from sqlalchemy import Column, Float, Integer, Text, DateTime
from sqlalchemy.orm.collections import InstrumentedList
from database import Base


class BaseModel(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True, comment='ID')

    def to_dict(self):
        data = dict()
        for k, v in self.__dict__.items():
            if k.startwith('_'):
                continue
            elif isinstance(v, Base):
                v = v.to_dict()
            elif isinstance(v, InstrumentedList):
                v = [item.to_dict() for item in v]
            data[k] = v
        return data


class XMessages(BaseModel):
    __tablename__ = 'xmessages'
    __table_args__ = {'comment': 'Подсчет Х в сообщениях.'}

    datetime_ = Column(DateTime(), nullable=False, comment='Дата и время')
    title = Column(Text(), nullable=False, comment='Заголовок')
    x_avg_count_in_line = Column(Float(), nullable=False, comment='Среднне число вхождений')

