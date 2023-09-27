from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime, DECIMAL, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class Charge(Base):
    __tablename__ = "Charges"
    __table_args__ = {"schema": "dbo"}

    Id: Mapped[int] = mapped_column(primary_key=True)
    RentGroup: Mapped[str] = mapped_column(String(3))
    PropertyRef: Mapped[str] = mapped_column(String(255))
    ChargePeriod: Mapped[str] = mapped_column(String(255))
    ChargeType: Mapped[str] = mapped_column(String(3))
    Amount: Mapped[float] = mapped_column(DECIMAL)
    Active: Mapped[bool] = mapped_column(Boolean)
    TimeStamp: Mapped[str] = mapped_column(DateTime)
    Year: Mapped[int] = mapped_column(INT)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"Charge(id={self.Id}, ChargeType={self.ChargeType}, " \
               f"Amount={self.Amount!r} PropertyRef={self.PropertyRef})"
