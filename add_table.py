from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Connect to the SQLite database
engine = create_engine('sqlite:///msr_target.db')

# Execute the ALTER TABLE command
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE carrier_selection ADD COLUMN discount_category VARCHAR"))
    conn.commit()