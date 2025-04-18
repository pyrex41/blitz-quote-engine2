from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import duckdb
import time

load_dotenv()

TURSO_DB_URL = os.getenv("NEW_QUOTE_DB_URL")
TURSO_DB_KEY = os.getenv("NEW_QUOTE_DB_KEY")

# Path to the DuckDB database
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_PATH", "./medicare.duckdb")

# Create a DuckDB connection with retry
duckdb_conn = None
max_retries = 5
retry_count = 0

while duckdb_conn is None and retry_count < max_retries:
    try:
        # Try to connect with read-only first, which is more likely to succeed
        duckdb_conn = duckdb.connect(DUCKDB_DATABASE_PATH, read_only=True)
        print(f"Successfully connected to DuckDB database at {DUCKDB_DATABASE_PATH} (read-only mode)")
    except Exception as e:
        retry_count += 1
        print(f"Attempt {retry_count}/{max_retries}: Error connecting to DuckDB: {str(e)}")
        if retry_count < max_retries:
            time.sleep(1)  # Wait before retrying
        else:
            print(f"Failed to connect to DuckDB after {max_retries} attempts. Using fallback mode.")
            # Set to None to indicate failed connection - we'll handle this in the code that uses it

# For compatibility with existing SQLAlchemy code, keep a SQLite connection
# This will be phased out as we move more queries to direct DuckDB
SQLALCHEMY_DATABASE_URL = "sqlite:///./msr_target.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_duckdb_conn():
    """Get DuckDB connection for direct queries"""
    global duckdb_conn
    if duckdb_conn is None:
        # Try to reconnect if the connection was lost
        try:
            duckdb_conn = duckdb.connect(DUCKDB_DATABASE_PATH, read_only=True)
            print("Reconnected to DuckDB database")
        except Exception as e:
            print(f"Error reconnecting to DuckDB: {str(e)}")
    return duckdb_conn
