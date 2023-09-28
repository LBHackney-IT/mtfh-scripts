from datetime import datetime

from _decimal import Decimal
from sqlalchemy.orm import Session

from aws.src.database.rds.housing_finance.entities.Charge import Charge
from aws.src.database.rds.housing_finance.entities.ChargesHistoryEntity import ChargesHistoryEntity
from aws.src.database.rds.housing_finance.entities.SSMiniTransaction import SSMiniTransaction


def heating_charge_adjustment_test(
        session: Session, prop_ref: str, fin_year_start: datetime,
        effective_date: datetime, adjustment_factor: Decimal):
    heating_trans_type = 'DHE'

    # == Charges ==
    charges = session.query(Charge) \
        .where(
        (Charge.PropertyRef == prop_ref) &
        (Charge.ChargeType == heating_trans_type) &
        (Charge.Active == True)
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
        (ChargesHistoryEntity.ChargeType == heating_trans_type) &
        (ChargesHistoryEntity.Date >= fin_year_start)
    ).order_by(ChargesHistoryEntity.Date.desc()).all()

    # == SSMiniTransaction ==
    ssmini_transaction = session.query(SSMiniTransaction) \
        .where(
        (SSMiniTransaction.prop_ref == prop_ref) &
        (SSMiniTransaction.trans_type == heating_trans_type) &
        (SSMiniTransaction.post_date >= fin_year_start)
    ).order_by(SSMiniTransaction.post_date.desc()).all()

    print("\n\n=== ChargesHistory ===")
    for charge in charges_history:
        if charge.Date < effective_date:
            print(f"\n{charge}")
            expected_amount = base_amount
            assert charge.Amount == expected_amount, \
                f"(BEFORE Eff Date) Charge amount incorrect: {charge.Amount} != {expected_amount}"

        elif charge.Date >= effective_date:
            print(f"\n{charge}")
            expected_amount = round(base_amount * adjustment_factor, 2)
            assert charge.Amount == expected_amount, \
                f"(AFTER Eff Date) Charge amount incorrect: {charge.Amount} != {expected_amount}"

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


def main_tests(
        session: Session, adjustable_propref: str, excluded_propref: str, fin_year_start: datetime,
        effective_date: datetime, adjustment_factor: Decimal):
    print("Heating charge adjustment tests...")
    # Should adjust for included propref
    heating_charge_adjustment_test(session, adjustable_propref, fin_year_start, effective_date, adjustment_factor)

    # Should not adjust for excluded propref
    heating_charge_adjustment_test(session, excluded_propref, fin_year_start, effective_date, Decimal(1.0))
    print("--------\nHeating Charge Adjustment PASSED\n--------")
