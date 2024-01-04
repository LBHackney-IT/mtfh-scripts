from dataclasses import dataclass, asdict

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class AddressStore(Base):
    __tablename__ = "address_store"
    __table_args__ = {"schema": "dbo"}

    address: Mapped[str] = mapped_column(primary_key=True)
    postcode: Mapped[str] = mapped_column(String)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"AddressStore(address={self.address!r}, postcode={self.postcode!r})"
