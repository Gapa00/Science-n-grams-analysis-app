# File: app/models/domain.py
from sqlalchemy import Column, Integer, String
from app.models.base import Base

class Domain(Base):
    __tablename__ = "domains"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)