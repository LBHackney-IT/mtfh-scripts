from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class GoogleFileSetting(Base):
    __tablename__ = "GoogleFileSetting"
    __table_args__ = {"schema": "dbo"}

    Id: Mapped[int] = mapped_column(primary_key=True)
    GoogleIdentifier: Mapped[str] = mapped_column(String(255))
    FileType: Mapped[str] = mapped_column(String(255))
    Label: Mapped[str] = mapped_column(String(255))
    StartDate: Mapped[str] = mapped_column(DateTime)
    EndDate: Mapped[str] = mapped_column(DateTime)
    FileYear: Mapped[int] = mapped_column(INT)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"GoogleFileSetting(id={self.Id}, GoogleIdentifier={self.GoogleIdentifier!r}, FileType={self.FileType})"
