from dataclasses import dataclass, asdict

from sqlalchemy import String, Integer
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class HackneyAddress(Base):
    __tablename__ = "hackney_address"
    __table_args__ = {"schema": "dbo"}

    lpi_key: Mapped[str] = mapped_column(primary_key=True)
    lpi_logical_status: Mapped[str] = mapped_column(String(18))
    lpi_start_date: Mapped[int] = mapped_column(Integer())
    lpi_end_date: Mapped[int] = mapped_column(Integer())
    lpi_last_update_date: Mapped[int] = mapped_column(Integer())
    usrn: Mapped[int] = mapped_column(Integer())
    uprn: Mapped[int] = mapped_column(Integer())
    parent_uprn: Mapped[int] = mapped_column(Integer())
    blpu_start_date: Mapped[int] = mapped_column(Integer())
    blpu_end_date: Mapped[int] = mapped_column(Integer())
    blpu_class: Mapped[str] = mapped_column(String(4))
    blpu_last_update_date: Mapped[int] = mapped_column(Integer())
    usage_description: Mapped[str] = mapped_column(String(160))
    usage_primary: Mapped[str] = mapped_column(String(160))
    property_shell: Mapped[bool] = mapped_column()
    easting: Mapped[float] = mapped_column()
    northing: Mapped[float] = mapped_column()
    unit_number: Mapped[int] = mapped_column(Integer())
    sao_text: Mapped[str] = mapped_column(String(90))
    building_number: Mapped[str] = mapped_column(String(17))
    pao_text: Mapped[str] = mapped_column(String(90))
    paon_start_num: Mapped[int] = mapped_column(Integer())
    street_description: Mapped[str] = mapped_column(String(100))
    locality: Mapped[str] = mapped_column(String(100))
    ward: Mapped[str] = mapped_column(String(100))
    town: Mapped[str] = mapped_column(String(100))
    postcode: Mapped[str] = mapped_column(String(8))
    postcode_nospace: Mapped[str] = mapped_column(String(8))
    planning_use_class: Mapped[str] = mapped_column(String(50))
    neverexport: Mapped[bool] = mapped_column()
    longitude: Mapped[float] = mapped_column()
    latitude: Mapped[float] = mapped_column()
    gazetteer: Mapped[str] = mapped_column(String(8))
    organisation: Mapped[str] = mapped_column(String(100))
    line1: Mapped[str] = mapped_column(String(200))
    line2: Mapped[str] = mapped_column(String(200))
    line3: Mapped[str] = mapped_column(String(200))
    line4: Mapped[str] = mapped_column(String(100))
    
    to_dict = asdict

    def __repr__(self) -> str:
        return f"Address(uprn={self.uprn!r}, line_1={self.line1!r}, line_2={self.line2!r})"
