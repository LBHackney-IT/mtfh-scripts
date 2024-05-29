from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class GoogleFileSetting(Base):
    __tablename__ = "googlefilesetting"
    # __table_args__ = {"schema": "dbo"}

    id: Mapped[int] = mapped_column(primary_key=True)
    googleidentifier: Mapped[str] = mapped_column(String(255))
    filetype: Mapped[str] = mapped_column(String(255))
    label: Mapped[str] = mapped_column(String(255))
    startdate: Mapped[str] = mapped_column(DateTime)
    enddate: Mapped[str] = mapped_column(DateTime)
    fileyear: Mapped[int] = mapped_column(INT)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"GoogleFileSetting(id={self.id}, GoogleIdentifier={self.googleidentifier!r}, FileType={self.filetype})"
