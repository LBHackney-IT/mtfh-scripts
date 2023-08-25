from dataclasses import dataclass, asdict

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class PropertyAlertNewEntity(Base):
    __tablename__ = "PropertyAlertNew"
    __table_args__ = {"schema": "dbo"}

    id: Mapped[int] = mapped_column(primary_key=True)
    door_number: Mapped[str] = mapped_column(String(10))
    address: Mapped[str] = mapped_column(String(255))
    neighbourhood: Mapped[str] = mapped_column(String(20))
    date_of_incident: Mapped[str] = mapped_column(String(12))
    code: Mapped[str] = mapped_column(String(5))
    caution_on_system: Mapped[str] = mapped_column(String(50))
    property_reference: Mapped[str] = mapped_column(String(12))
    uprn: Mapped[str] = mapped_column(String(12))
    person_name: Mapped[str] = mapped_column(String(255))
    mmh_id: Mapped[str] = mapped_column(String(36))
    outcome: Mapped[str] = mapped_column(String(2000))
    assure_reference: Mapped[str] = mapped_column(String(12))
    is_active: Mapped[bool] = mapped_column()
    alert_id: Mapped[str] = mapped_column(String(36))
    
    to_dict = asdict

    def __repr__(self) -> str:
        return f"PropAlert(id={self.id!r}, alert_id={self.alert_id!r}, fullname={self.person_name!r})"
