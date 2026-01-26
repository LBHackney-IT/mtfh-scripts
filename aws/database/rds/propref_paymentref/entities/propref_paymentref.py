"""Entity definition for the propref_paymentref table"""

from dataclasses import asdict, dataclass

from sqlalchemy import TIMESTAMP, String
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class ProprefPaymentref(Base):
    __tablename__ = "propref_paymentref"

    Id: Mapped[int] = mapped_column(primary_key=True)
    PropertyRefNumber: Mapped[str] = mapped_column(String(50))
    PaymentRefNumber: Mapped[str] = mapped_column(String(50))
    CreatedAt: Mapped[str] = mapped_column(TIMESTAMP)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"ProprefPaymentref(Id={self.Id}, PropertyRefNumber={self.PropertyRefNumber!r}, PaymentRefNumber={self.PaymentRefNumber!r})"
