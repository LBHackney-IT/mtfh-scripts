from dataclasses import dataclass, asdict

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class Base(MappedAsDataclass, DeclarativeBase):
    pass


@dataclass
class BudgetCode(Base):
    __tablename__ = "budget_codes"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    corporate_subjective_code: Mapped[str] = mapped_column(String)
    external_cost_code: Mapped[str] = mapped_column(String)
    cost_code: Mapped[str] = mapped_column(String, nullable=True)

    to_dict = asdict

    def __repr__(self) -> str:
        return (
            f"BudgetCode(corporate_subjective_code={self.corporate_subjective_code!r}, "
            f"external_cost_code={self.external_cost_code!r}, cost_code={self.cost_code!r})"
        )