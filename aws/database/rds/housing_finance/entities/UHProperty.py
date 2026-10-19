from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from aws.database.rds.housing_finance.entities.Base import Base


class UHProperty(Base):
    __tablename__ = "UHProperty"

    prop_ref: Mapped[str] = mapped_column(String(12), primary_key=True)
    major_ref: Mapped[str] = mapped_column(String(12))
    man_scheme: Mapped[str] = mapped_column(String(11))
    post_code: Mapped[str] = mapped_column(String(10))
    short_address: Mapped[str] = mapped_column(String(200))
    telephone: Mapped[str] = mapped_column(String(21))
    ownership: Mapped[str] = mapped_column(String(10))
    agent: Mapped[str] = mapped_column(String(3))
    area_office: Mapped[str] = mapped_column(String(3))
    subtyp_code: Mapped[str] = mapped_column(String(3))
    letable: Mapped[bool] = mapped_column()
    cat_type: Mapped[str] = mapped_column(String(3))
    house_ref: Mapped[str] = mapped_column(String(10))
    occ_stat: Mapped[str] = mapped_column(String(3))
    post_preamble: Mapped[str] = mapped_column(String(60))
    property_sid: Mapped[int] = mapped_column()
    arr_patch: Mapped[str] = mapped_column(String(3))
    address1: Mapped[str] = mapped_column(String(255))
    num_bedrooms: Mapped[int | None] = mapped_column()
    post_desig: Mapped[str] = mapped_column(String(60))
