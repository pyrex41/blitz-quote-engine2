from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, JSON, VARCHAR, TEXT, INTEGER
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index

Base = declarative_base()

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

# ... existing models ...

class CarrierSelection(Base):
    __tablename__ = 'carrier_selection'

    naic = Column(VARCHAR, primary_key=True)
    company_name = Column(VARCHAR)
    selected = Column(INTEGER)  # Using INTEGER for boolean (0/1)
    discount_category = Column(VARCHAR)