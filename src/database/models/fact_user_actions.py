from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Index
from .base import Base

class FactUserAction(Base):
    __tablename__ = "fact_user_actions"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    user_id = Column(String(64), ForeignKey("dim_users.user_id"), nullable=False)
    action_id = Column(Integer, ForeignKey("dim_actions.action_id"), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("idx_user_action_time", "user_id", "action_id", "timestamp"),
    )

    def __repr__(self) -> str:
        return (f"<FactUserAction(id={self.id}, user_id='{self.user_id}', "
                f"action_id={self.action_id}, timestamp='{self.timestamp}')>")
