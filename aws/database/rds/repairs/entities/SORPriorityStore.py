from dataclasses import dataclass, asdict
from typing import Optional

from sqlalchemy import String, Boolean, Integer
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column
from sqlalchemy.exc import NoResultFound, MultipleResultsFound


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class SORPriority(Base):
    __tablename__ = "sor_priorities"
    __table_args__ = {"schema": "public"}

    priority_code: Mapped[int] = mapped_column(primary_key=True, init=False)
    description: Mapped[str] = mapped_column(String)
    priority_character: Mapped[str] = mapped_column(String)
    days_to_complete: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean)
    display_order: Mapped[int] = mapped_column(Integer, default=0)

    to_dict = asdict

    def __repr__(self) -> str:
        return (
            f"SORPriority(priority_code={self.priority_code!r}, "
            f"description={self.description!r}, enabled={self.enabled!r})"
        )