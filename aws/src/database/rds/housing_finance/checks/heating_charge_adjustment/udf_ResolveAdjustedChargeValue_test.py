from dataclasses import dataclass
from datetime import datetime, timedelta

from _decimal import Decimal
from sqlalchemy import text
from sqlalchemy.orm import Session

from aws.src.database.rds.housing_finance.session_for_hfs import session_for_hfs
from enums.enums import Stage

ADJUSTED_PROPREF = '00001618'
EXCLUDED_PROPREF = '00005457'
CUTOFF_DATE = datetime(2023, 6, 2)

TEST_CHARGE_AMOUNT = Decimal(100.00)


@dataclass
class FuncArgs:
    property_ref: str
    charge_amount: Decimal
    charge_type: str
    charge_date: datetime
    days_diff: int = 7


def get_result(session: Session, prop_ref: str, charge_date: datetime) -> Decimal:
    test_charge = FuncArgs(
        property_ref=prop_ref,
        charge_amount=TEST_CHARGE_AMOUNT,
        charge_type='DHE',
        charge_date=charge_date,
        days_diff=7
    )
    query = text(
        f'SELECT dbo.udf_ResolveAdjustedChargeValue(:property_ref, :charge_amount, :charge_type, :charge_date, :days_diff)')

    # Call user defined function with test data
    result = session.execute(query, {
        'property_ref': test_charge.property_ref,
        'charge_amount': test_charge.charge_amount,
        'charge_type': test_charge.charge_type,
        'charge_date': test_charge.charge_date,
        'days_diff': test_charge.days_diff
    }).fetchone()[0]
    # Round to 2 decimal places
    result: Decimal = result.quantize(Decimal('.01'))
    return result


def does_not_modify_charges_before_cutoff_date(session: Session, prop_ref: str):
    past_charge_date = CUTOFF_DATE - timedelta(days=1)
    result = get_result(session, prop_ref, past_charge_date)
    expected_amount = TEST_CHARGE_AMOUNT

    print("\nDoes not modify charges before cutoff date")
    assert result == expected_amount, \
        f"FAIL Charge amount is not as expected: {result} != {expected_amount}"
    print(f"PASS {result} -> {expected_amount}")


def adjusts_charges_after_cutoff_date(session: Session, prop_ref: str):
    # Call user defined function with test data
    future_charge_date = CUTOFF_DATE + timedelta(days=1)
    result = get_result(session, prop_ref, future_charge_date)
    result = round(result, 2)
    expected_amount = round(TEST_CHARGE_AMOUNT * Decimal(0.94), 2)
    print("\nAdjusts charges after cutoff date")
    assert result == expected_amount, \
        f"FAIL Charge amount is not as expected: {result} != {expected_amount}"
    print(f"PASS {result} -> {expected_amount}")


def does_not_modify_excluded_propref(session: Session, prop_ref: str):
    result = get_result(session, prop_ref, CUTOFF_DATE + timedelta(days=1))
    expected_amount = TEST_CHARGE_AMOUNT
    print("\nDoes not modify excluded propref")
    assert result == expected_amount, \
        f"FAIL Charge amount is not as expected: {result} != {expected_amount}"
    print(f"PASS {result} -> {expected_amount}")


if __name__ == '__main__':
    HfsSession = session_for_hfs(Stage.HOUSING_DEVELOPMENT)
    with HfsSession.begin() as sess:
        does_not_modify_charges_before_cutoff_date(sess, ADJUSTED_PROPREF)
        adjusts_charges_after_cutoff_date(sess, ADJUSTED_PROPREF)
        does_not_modify_excluded_propref(sess, EXCLUDED_PROPREF)
