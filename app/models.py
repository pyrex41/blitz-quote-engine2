from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, JSON, VARCHAR, TEXT, INTEGER, FLOAT, TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index
from datetime import datetime

Base = declarative_base()

# Legacy models for compatibility with existing code
class GroupType(Base):
    __tablename__ = 'group_type'

    naic = Column(TEXT, primary_key=True)
    state = Column(TEXT, primary_key=True)
    group_zip = Column(INTEGER, primary_key=False)

class RateStore(Base):
    __tablename__ = 'rate_store'

    key = Column(TEXT, primary_key=True, index=True)
    effective_date = Column(TEXT, primary_key=False, index=True)
    value = Column(JSON, primary_key=False)

class CompanyNames(Base):
    __tablename__ = 'company_names'

    id = Column(INTEGER, primary_key=True)
    naic = Column(VARCHAR, primary_key=False, index=True)
    name = Column(VARCHAR, primary_key=False)

class GroupMapping(Base):
    __tablename__ = 'group_mapping'

    naic = Column(TEXT, primary_key=True)
    state = Column(TEXT, primary_key=True)
    location = Column(TEXT, primary_key=True)
    naic_group = Column(INTEGER, primary_key=False)

    __table_args__ = (
        Index('idx_naic_state_location', 'naic', 'state', 'location'),
    )

class CarrierSelection(Base):
    __tablename__ = 'carrier_selection'

    naic = Column(VARCHAR, primary_key=True)
    company_name = Column(VARCHAR)
    selected = Column(INTEGER)  # Using INTEGER for boolean (0/1)
    discount_category = Column(VARCHAR)

# New models for DuckDB schema
class DuckDBRateStore:
    """Model representing rate_store table in DuckDB (not SQLAlchemy model)"""
    table_name = 'rate_store'
    primary_keys = ['region_id', 'gender', 'tobacco', 'age', 'naic', 'plan', 'effective_date', 'state']
    
    @staticmethod
    def get_most_recent_effective_date_query():
        """Get SQL query to find most recent effective date not after requested date"""
        return """
        SELECT r.*
        FROM rate_store r
        WHERE r.region_id = ?
          AND r.gender = ?
          AND r.tobacco = ?
          AND r.age = ?
          AND r.naic = ?
          AND r.plan = ?
          AND r.state = ?
          AND r.effective_date = (
            SELECT MAX(effective_date) 
            FROM rate_store
            WHERE region_id = ?
              AND gender = ?
              AND tobacco = ?
              AND age = ?
              AND naic = ?
              AND plan = ?
              AND state = ?
              AND CAST(effective_date AS DATE) <= CAST(? AS DATE)
          )
        """

class DuckDBCarrierInfo:
    """Model representing carrier_info table in DuckDB (not SQLAlchemy model)"""
    table_name = 'carrier_info'
    primary_keys = ['naic']

class DuckDBRegionMapping:
    """Model representing region_mapping table in DuckDB (not SQLAlchemy model)"""
    table_name = 'region_mapping'
    primary_keys = ['zip_code', 'naic']

class DuckDBRegionMetadata:
    """Model representing region_metadata table in DuckDB (not SQLAlchemy model)"""
    table_name = 'region_metadata'
    primary_keys = ['region_id']