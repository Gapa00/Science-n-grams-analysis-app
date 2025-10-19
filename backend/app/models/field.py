# File: app/models/field.py
from sqlalchemy import Column, Integer, String, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from app.models.base import Base

class Field(Base):
    __tablename__ = "fields"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    domain_id = Column(Integer, ForeignKey("domains.id"), nullable=False)

    domain = relationship("Domain", backref="fields")

    __table_args__ = (
        UniqueConstraint("name", "domain_id", name="uq_field_name_domain"),
        Index("idx_field_domain", "domain_id"),
    )