from sqlalchemy import Column, Integer, String, JSON
from .database import Base
from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import relationship

class Rate(Base):
    __tablename__ = "rate_store"

    key = Column(String, primary_key=True)
    value = Column(JSON)
    effective_date = Column(String)

class GroupMapping(Base):
    __tablename__ = "group_mapping"

    naic = Column(String, primary_key=True)
    state = Column(String, primary_key=True)
    location = Column(String, primary_key=True)
    naic_group = Column(Integer)

class CompanyName(Base):
    __tablename__ = 'company_names'
    id = Column(Integer, primary_key=True)
    naic = Column(String, ForeignKey('group_mapping.naic'), unique=True, nullable=False)
    name = Column(String, nullable=False)

    __table_args__ = (Index('idx_company_names_naic', 'naic'),)