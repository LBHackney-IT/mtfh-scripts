from dataclasses import dataclass

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column
from aws.database.rds.repairs.entities.Base import Base


@dataclass
class Person(Base):
    __tablename__ = "person"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_status_update_id: Mapped[int] = mapped_column()
    identification_number: Mapped[str] = mapped_column(String)
    identification_type: Mapped[str] = mapped_column(String)
    name_full: Mapped[str] = mapped_column(String)
    name_family: Mapped[str] = mapped_column(String)
    name_family_prefix: Mapped[str] = mapped_column(String)
    name_given: Mapped[str] = mapped_column(String)
    name_initials: Mapped[str] = mapped_column(String)
    name_middle: Mapped[str] = mapped_column(String)
    name_title: Mapped[str] = mapped_column(String)
    calculated_bonus: Mapped[float] = mapped_column()

    def __repr__(self) -> str:
        return f"Person(id={self.id!r}, name_full={self.name_full!r})"
