from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime, DECIMAL, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class ChargesHistoryAdjustmentsExclusion(Base):
    __tablename__ = "ChargesHistoryAdjustmentsExclusion"
    __table_args__ = {"schema": "dbo"}

    Id: Mapped[int] = mapped_column(primary_key=True)
    ExclusionSetRef: Mapped[int] = mapped_column(INT)
    PropertyRef: Mapped[str] = mapped_column(String(12))

    to_dict = asdict

    def __repr__(self) -> str:
        return f"CHA_Exclusion(id={self.Id}, ExclusionSetRef={self.ExclusionSetRef}, PropertyRef={self.PropertyRef})"
