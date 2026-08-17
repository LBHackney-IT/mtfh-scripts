from dataclasses import dataclass, asdict

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column

class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class Trade(Base):
    __tablename__ = "trades" 
    __table_args__ = {"schema": "public"}

    code: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)

    to_dict = asdict

    def __repr__(self) -> str:
        return f"SorCodeTrade(code={self.code!r}, name={self.name!r})"