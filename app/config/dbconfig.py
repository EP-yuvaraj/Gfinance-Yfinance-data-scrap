# Database Connection is here
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2.extras


DATABASE_URL = "postgresql://postgres:yuviboxer@192.168.99.1/gfinance_yfinance_db"


engine=create_engine(DATABASE_URL,echo=True)

sessionLocal = sessionmaker(bind=engine)

conn=psycopg2.connect('postgresql://postgres:yuviboxer@192.168.99.1/gfinance_yfinance_db')

Base = declarative_base()

# Base.metadata.create_all(engine)
# print("Created DataBase ....")