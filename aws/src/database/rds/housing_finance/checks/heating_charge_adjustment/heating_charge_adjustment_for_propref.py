from datetime import datetime

from _decimal import Decimal

from aws.src.database.rds.housing_finance.entities.Charge import Charge
from aws.src.database.rds.housing_finance.entities.ChargesHistoryEntity import ChargesHistoryEntity
from aws.src.database.rds.housing_finance.entities.SSMiniTransaction import SSMiniTransaction
from aws.src.database.rds.housing_finance.session_for_hfs import session_for_hfs
from enums.enums import Stage

ADJUSTABLE_PROPREF = input("Enter adjustable propref: ")
EXCLUDED_PROPREF = input("Enter excluded propref: ")

STAGE = Stage.HOUSING_DEVELOPMENT
FIN_YEAR_START = datetime(2023, 4, 1)
EFFECTIVE_DATE = datetime(2023, 6, 2)
HEATING_TRANS_TYPE = 'DHE'
ADJUSTMENT_FACTOR = Decimal(0.94)


def heating_charge_adjustment_test(
        prop_ref: str, fin_year_start: datetime, effective_date: datetime, adjustment_factor: Decimal):
    HfsSession = session_for_hfs(STAGE)
    with HfsSession.begin() as session:
        # == Charges ==
        charges = session.query(Charge) \
            .where(
            (Charge.PropertyRef == prop_ref) &
            (Charge.ChargeType == HEATING_TRANS_TYPE) &
            (Charge.Active.is_(True))
        ).all()
        print("\n\n=== Charges ===")
        for charge in charges:
            print(charge)
        base_amount = charges[0].Amount
        print(f"base_amount: {base_amount}")

        # == ChargesHistory ==
        charges_history = session.query(ChargesHistoryEntity) \
            .where(
            (ChargesHistoryEntity.PropertyRef == prop_ref) &
            (ChargesHistoryEntity.ChargeType == HEATING_TRANS_TYPE) &
            (ChargesHistoryEntity.Date >= fin_year_start)
        ).order_by(ChargesHistoryEntity.Date.desc()).all()

        print("\n\n=== ChargesHistory ===")
        for charge in charges_history:
            if charge.Date < effective_date:
                print(f"\n{charge}")
                expected_amount = base_amount
                assert charge.Amount == expected_amount, \
                    f"(BEFORE Eff Date) Charge amount incorrect: {charge.Amount} != {expected_amount}"
                print(f"(BEFORE Eff Date) Charge amount correct: {charge.Amount} == {expected_amount}")
            elif charge.Date >= effective_date:
                print(f"\n{charge}")
                expected_amount = round(base_amount * adjustment_factor, 2)
                assert charge.Amount == expected_amount, \
                    f"(AFTER Eff Date) Charge amount incorrect: {charge.Amount} != {expected_amount}"
                print(f"(AFTER Eff Date) Charge amount correct: {base_amount} == {expected_amount}")

        # == SSMiniTransaction ==
        ssmini_transaction = session.query(SSMiniTransaction) \
            .where(
            (SSMiniTransaction.prop_ref == prop_ref) &
            (SSMiniTransaction.trans_type == HEATING_TRANS_TYPE) &
            (SSMiniTransaction.post_date >= fin_year_start)
        ).order_by(SSMiniTransaction.post_date.desc())

        print("\n\n=== SSMiniTransaction ===")
        for transaction in ssmini_transaction:
            print(f"\n{transaction}")
            if transaction.post_date < effective_date:
                expected_amount = base_amount
                assert transaction.real_value == expected_amount, \
                    f"(BEFORE Eff Date) Transaction amount incorrect: {transaction.real_value} != {expected_amount}"
                print(f"(BEFORE Eff Date) Transaction amount correct: {base_amount} == {expected_amount}")
            elif transaction.post_date >= effective_date:
                expected_amount = round(base_amount * adjustment_factor, 2)
                assert transaction.real_value == expected_amount, \
                    f"(AFTER Eff Date) Transaction amount incorrect: {transaction.real_value} != {expected_amount}"
                print(f"(AFTER Eff Date) Transaction amount correct: {base_amount} -> {expected_amount}")
    print("--------\nTests PASSED\n--------")


if __name__ == "__main__":
    heating_charge_adjustment_test(EXCLUDED_PROPREF, FIN_YEAR_START, EFFECTIVE_DATE, ADJUSTMENT_FACTOR)
