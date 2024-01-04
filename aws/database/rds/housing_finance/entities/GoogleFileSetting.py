from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


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
