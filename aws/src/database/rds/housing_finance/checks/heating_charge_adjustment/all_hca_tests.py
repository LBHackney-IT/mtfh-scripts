from datetime import datetime

from _decimal import Decimal

from aws.src.database.rds.housing_finance.session_for_hfs import session_for_hfs
from enums.enums import Stage
from heating_charge_adjustment_for_propref import main_tests as heating_charge_adjustment_tests
from udf_ResolveAdjustedChargeValue_test import main_tests as udf_tests

ADJUSTABLE_PROPREF = input("Enter adjustable propref: ")
EXCLUDED_PROPREF = input("Enter excluded propref: ")

STAGE = Stage.HOUSING_DEVELOPMENT
FIN_YEAR_START = datetime(2023, 4, 1)
EFFECTIVE_DATE = datetime(2023, 6, 2)
HEATING_TRANS_TYPE = 'DHE'
ADJUSTMENT_FACTOR = Decimal(0.94)

TEST_CHARGE_AMOUNT = Decimal(100.00)
ADJUSTMENT_EFFECTIVE_DATE = datetime(2023, 6, 2)


def main():
    HfsSession = session_for_hfs(STAGE)
    with HfsSession.begin() as session:
        print("Running main tests...")
        heating_charge_adjustment_tests(
            session, ADJUSTABLE_PROPREF, EXCLUDED_PROPREF, FIN_YEAR_START, EFFECTIVE_DATE, ADJUSTMENT_FACTOR
        )
        udf_tests(session, ADJUSTABLE_PROPREF, EXCLUDED_PROPREF, TEST_CHARGE_AMOUNT, ADJUSTMENT_EFFECTIVE_DATE)
        print("--------\nAll tests PASSED\n--------")


if __name__ == '__main__':
    main()
