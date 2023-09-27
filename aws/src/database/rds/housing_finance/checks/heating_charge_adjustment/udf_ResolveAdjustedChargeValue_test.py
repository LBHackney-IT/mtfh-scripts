from dataclasses import dataclass
from datetime import datetime, timedelta

from _decimal import Decimal
from sqlalchemy import text
from sqlalchemy.orm import Session

from aws.src.database.rds.housing_finance.session_for_hfs import session_for_hfs
from enums.enums import Stage

ADJUSTABLE_PROPREF = input("Enter adjustable propref: ")
EXCLUDED_PROPREF = input("Enter excluded propref: ")
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


def check_if_propref_gets_adjusted(session: Session, prop_ref: str, should_adjust: bool):
    # Does not modify charges before cutoff date
    past_charge_date = CUTOFF_DATE - timedelta(days=1)
    result = get_result(session, prop_ref, past_charge_date)
    expected_amount = TEST_CHARGE_AMOUNT
    assert result == expected_amount, \
        f"FAIL Modified charge before cutoff date {CUTOFF_DATE}: {result} != {expected_amount}"

    # Adjusts charges after cutoff date (if expected)
    if should_adjust:
        expected_amount = round(TEST_CHARGE_AMOUNT * Decimal(0.94), 2)
    else:
        expected_amount = TEST_CHARGE_AMOUNT
    future_charge_date = CUTOFF_DATE + timedelta(days=1)
    result = get_result(session, prop_ref, future_charge_date)

    assert result == expected_amount, \
        f"FAIL Charge amount is not as expected: {result} != {expected_amount}"


def main_tests():
    HfsSession = session_for_hfs(Stage.HOUSING_DEVELOPMENT)
    with HfsSession.begin() as sess:
        check_if_propref_gets_adjusted(sess, ADJUSTABLE_PROPREF, should_adjust=True)
        check_if_propref_gets_adjusted(sess, EXCLUDED_PROPREF, should_adjust=False)


if __name__ == '__main__':
    main_tests()
