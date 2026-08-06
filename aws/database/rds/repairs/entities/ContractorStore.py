from dataclasses import dataclass, asdict
from typing import Optional

from sqlalchemy import String, Boolean
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy.exc import NoResultFound, MultipleResultsFound


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True):
    pass


@dataclass
class Contractor(Base):
    __tablename__ = "contractors"
    __table_args__ = {"schema": "public"}

    reference: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    use_external_schedule_manager: Mapped[bool] = mapped_column(Boolean)
    can_assign_operative: Mapped[bool] = mapped_column(Boolean)
    contract_manager_email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    per_trade_availability: Mapped[bool] = mapped_column(Boolean, default=False)
    multi_trade_enabled: Mapped[bool] = mapped_column(Boolean, default=False)

    to_dict = asdict

    def __repr__(self) -> str:
        return (
            f"Contractor(reference={self.reference!r}, name={self.name!r}, "
            f"can_assign_operative={self.can_assign_operative!r})"
        )