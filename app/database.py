from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
#import libsql_experimental as libsql

load_dotenv()

TURSO_DB_URL = os.getenv("NEW_QUOTE_DB_URL")
TURSO_DB_KEY = os.getenv("NEW_QUOTE_DB_KEY")

dbUrl = f"sqlite+{TURSO_DB_URL}/?authToken={TURSO_DB_KEY}&secure=true"

#conn = libsql.connect("msr_replica.db", sync_url=TURSO_DB_URL, auth_token=TURSO_DB_KEY)
#conn.sync()


SQLALCHEMY_DATABASE_URL = "sqlite:///./msr_target.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
#engine = create_engine(dbUrl, connect_args={'check_same_thread': False}, echo=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
