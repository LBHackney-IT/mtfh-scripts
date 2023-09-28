from dataclasses import dataclass
from datetime import datetime, timedelta

from _decimal import Decimal
from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class FuncArgs:
    property_ref: str
    charge_amount: Decimal
    charge_type: str
    charge_date: datetime
    days_diff: int = 7


def get_result(session: Session, prop_ref: str, charge_date: datetime, test_charge_amount: Decimal) -> Decimal:
    test_charge = FuncArgs(
        property_ref=prop_ref,
        charge_amount=test_charge_amount,
        charge_type='DHE',
        charge_date=charge_date,
        days_diff=7
    )
    query = text(
        'SELECT dbo.udf_ResolveAdjustedChargeValue(:property_ref, '
        ':charge_amount, :charge_type, :charge_date, :days_diff)')

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


def check_if_propref_gets_adjusted(
        session: Session, prop_ref: str, should_adjust: bool,
        test_charge_amount: Decimal, adjustment_effective_date: datetime):
    # Does not modify charges before cutoff date
    past_charge_date = adjustment_effective_date - timedelta(days=1)
    result = get_result(session, prop_ref, past_charge_date, test_charge_amount)
    expected_amount = test_charge_amount
    assert result == expected_amount, \
        f"FAIL Modified charge before cutoff date {adjustment_effective_date}: {result} != {expected_amount}"

    # Adjusts charges after cutoff date (if expected)
    if should_adjust:
        expected_amount = round(test_charge_amount * Decimal(0.94), 2)
    else:
        expected_amount = test_charge_amount
    future_charge_date = adjustment_effective_date + timedelta(days=1)
    result = get_result(session, prop_ref, future_charge_date, test_charge_amount)

    assert result == expected_amount, \
        f"FAIL Charge amount is not as expected: {result} != {expected_amount}"


def main_tests(
        sess: Session, adjustable_propref: str, excluded_propref: str,
        test_charge_amount: Decimal, adjustment_effective_date: datetime):
    print("Running udf_ResolveAdjustedChargeValue tests...")
    check_if_propref_gets_adjusted(sess, adjustable_propref, True, test_charge_amount, adjustment_effective_date)
    check_if_propref_gets_adjusted(sess, excluded_propref, False, test_charge_amount, adjustment_effective_date)
    print("--------\nudf_ResolveAdjustedChargeValue tests PASSED\n--------")
