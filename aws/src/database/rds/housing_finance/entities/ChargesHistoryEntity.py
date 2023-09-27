from dataclasses import dataclass, asdict
from datetime import datetime

from sqlalchemy import String, INT, DateTime, DECIMAL, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class ChargesHistoryEntity(Base):
    __tablename__ = "ChargesHistory"
    __table_args__ = {"schema": "dbo"}

    Id: Mapped[int] = mapped_column(primary_key=True)
    TenancyAgreementRef: Mapped[str] = mapped_column(String(255))
    PropertyRef: Mapped[str] = mapped_column(String(255))
    ChargePeriod: Mapped[str] = mapped_column(String(255))
    Date: Mapped[datetime] = mapped_column(DateTime)
    IsRead: Mapped[bool] = mapped_column(Boolean)
    ChargesId: Mapped[int] = mapped_column(INT)
    ChargeType: Mapped[str] = mapped_column(String(3))
    Amount: Mapped[float] = mapped_column(DECIMAL)
    TimeStamp: Mapped[str] = mapped_column(DateTime)
    FirstWeekAdjustment: Mapped[bool] = mapped_column(Boolean)
    LastWeekAdjustment: Mapped[bool] = mapped_column(Boolean)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"ChargesHistoryEntity(id={self.Id}, ChargeType={self.ChargeType}, " \
               f"Amount={self.Amount!r}, Date={self.Date.date()})"
