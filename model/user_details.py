from sqlalchemy import create_engine,Column,Integer,String,Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class UserDetail(Base):
    __tablename__ = 'UserDetails'
    id = Column(Integer,primary_key=True)
    Username = Column(String(50))
    age = Column(Integer)

    