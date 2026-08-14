from dataclasses import dataclass, asdict
from typing import Optional

from sqlalchemy import String, Boolean, Integer, Float
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy.exc import NoResultFound, MultipleResultsFound


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True):
    pass


@dataclass
class SorCode(Base):
    __tablename__ = "sor_codes"
    __table_args__ = {"schema": "public"}

    code: Mapped[str] = mapped_column(String, primary_key=True)
    short_description: Mapped[str] = mapped_column(String)
    long_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    priority_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    trade_code: Mapped[str] = mapped_column(String)
    enabled: Mapped[bool] = mapped_column(Boolean)
    is_raisable: Mapped[bool] = mapped_column(Boolean)
    display_priority: Mapped[int] = mapped_column(Integer)
    standard_minute_value: Mapped[int] = mapped_column(Integer, default=0)
    is_outofhours: Mapped[bool] = mapped_column(Boolean, default=False)
    operative_cost: Mapped[float] = mapped_column(Float, default=0)
    income: Mapped[float] = mapped_column(Float, default=0)

    to_dict = asdict

    def __repr__(self) -> str:
        return (
            f"SorCode(code={self.code!r}, short_description={self.short_description!r}, "
            f"trade_code={self.trade_code!r}, enabled={self.enabled!r})"
        )