from dataclasses import dataclass, asdict

from sqlalchemy import String, INT, DateTime, DECIMAL, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from aws.src.database.rds.housing_finance.entities.Base import Base


@dataclass
class SSMiniTransaction(Base):
    __tablename__ = "SSMiniTransaction"
    __table_args__ = {"schema": "dbo"}

    tag_ref: Mapped[str] = mapped_column(String(11))
    prop_ref: Mapped[str] = mapped_column(String(12))
    rentgroup: Mapped[str] = mapped_column(String(3))
    post_year: Mapped[int] = mapped_column(INT)
    post_prdno: Mapped[float] = mapped_column(DECIMAL(3))
    tenure: Mapped[str] = mapped_column(String(3))
    trans_type: Mapped[str] = mapped_column(String(3))
    trans_src: Mapped[str] = mapped_column(String(3))
    real_value: Mapped[float] = mapped_column(DECIMAL(9, 2))
    post_date: Mapped[str] = mapped_column(DateTime)
    trans_ref: Mapped[str] = mapped_column(String(12))
    origin: Mapped[str] = mapped_column(String(255), primary_key=True)
    origin_desc: Mapped[str] = mapped_column(String(255))

    to_dict = asdict

    def __repr__(self) -> str:
        return f"ChargesHistoryEntity(origin={self.origin}, trans_type={self.trans_type}, real_value={self.real_value!r}, post_date={self.post_date})"
