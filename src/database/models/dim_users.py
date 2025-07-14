from sqlalchemy import Column, String
from .base import Base

class DimUser(Base):
    __tablename__ = "dim_users"

    user_id = Column(String(64), primary_key=True, index=True, nullable=False)
    device = Column(String(100), nullable=True)
    location = Column(String(100), nullable=True)

    def __repr__(self) -> str:
        return f"<DimUser(user_id='{self.user_id}', device='{self.device}', location='{self.location}')>"
