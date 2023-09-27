from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime, DECIMAL, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class ChargesHistoryAdjustment(Base):
    __tablename__ = "ChargesHistoryAdjustments"
    __table_args__ = {"schema": "dbo"}

    Id: Mapped[int] = mapped_column(primary_key=True)
    StartDate: Mapped[DateTime] = mapped_column(DateTime)
    EndDate: Mapped[DateTime] = mapped_column(DateTime)
    ChargeType: Mapped[str] = mapped_column(String(3))
    AdjustmentFactor: Mapped[DECIMAL] = mapped_column(DECIMAL(5, 4))
    Description: Mapped[str] = mapped_column(String(255))
    ExclusionSetRef: Mapped[int] = mapped_column(INT)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"ChargesHistoryAdjustmentsExclusion(" \
               f"Id={self.Id}, ExSet={self.ExclusionSetRef}, SD={self.StartDate}, ED={self.EndDate}"
