import datetime
from typing import Any

from mypy_boto3_dynamodb.service_resource import Table
from progress.bar import Bar
from sqlalchemy import select, tuple_

from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.database.rds.propref_paymentref.entities.propref_paymentref import (
    ProprefPaymentref,
)
from aws.database.rds.propref_paymentref.session_for_propref_paymentref import (
    session_for_propref_paymentref,
)
from enums.enums import Stage

STAGE = Stage.HOUSING_DEVELOPMENT


def restore_propref_paymentref():
    """
    1. Iteratively scan the TenureInformation table for items with a propref and paymentref
    2. For each item, update the propref_paymentref table with the propref and paymentref values
    """
    aws_session = get_session_for_stage(STAGE)
    tenure_table: Table = aws_session.resource("dynamodb").Table("TenureInformation")
    scan_kwargs: dict[str, Any] = {
        "ProjectionExpression": "id, tenuredAsset, paymentReference",
    }

    start_key = None
    total_estimated = tenure_table.item_count
    progress_bar = Bar("Restoring propref_paymentref", max=total_estimated)

    Session = session_for_propref_paymentref(STAGE)
    with Session() as session:
        while True:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key

            response = tenure_table.scan(**scan_kwargs)
            items = response.get("Items", [])

            # 1. Collect all candidates from this page
            candidate_map = {}
            for item in items:
                tenured_asset = item.get("tenuredAsset", {})
                propref = (
                    tenured_asset.get("propertyReference")
                    if isinstance(tenured_asset, dict)
                    else None
                )
                paymentref = item.get("paymentReference")

                if propref and paymentref:
                    # Map unique pairs to avoid processing duplicates within the same batch
                    candidate_map[(str(propref), str(paymentref))] = item

                progress_bar.next()

            if candidate_map:
                # 2. Find which pairs already are missing in RDS for this batch
                existing_pairs = session.execute(
                    select(
                        ProprefPaymentref.PropertyRefNumber,
                        ProprefPaymentref.PaymentRefNumber,
                    ).where(
                        tuple_(
                            ProprefPaymentref.PropertyRefNumber,
                            ProprefPaymentref.PaymentRefNumber,
                        ).in_(list(candidate_map.keys()))
                    )
                ).all()

                existing_set = set((row[0], row[1]) for row in existing_pairs)
                missing_set = set(candidate_map.keys()) - existing_set

                # 3. Add only the missing records
                now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                for prop, pay in missing_set:
                    propref_paymentref = ProprefPaymentref(
                        PropertyRefNumber=prop[:50],
                        PaymentRefNumber=pay[:50],
                        CreatedAt=now,
                    )
                    session.add(propref_paymentref)

                session.commit()

            start_key = response.get("LastEvaluatedKey", None)
            if not start_key:
                break

    progress_bar.finish()


if __name__ == "__main__":
    restore_propref_paymentref()
