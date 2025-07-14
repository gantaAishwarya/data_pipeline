from sqlalchemy import Column, Integer, String
from .base import Base

class DimAction(Base):
    __tablename__ = "dim_actions"

    action_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    action_type = Column(String(255), unique=True, nullable=False)

    def __repr__(self) -> str:
        return f"<DimAction(action_id={self.action_id}, action_type='{self.action_type}')>"
