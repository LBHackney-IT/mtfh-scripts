"""
A script to report the count of each assetType in the Assets DynamoDB table.

The table is scanned in pages, projecting only the assetType attribute, and the
counts are printed to the console sorted by frequency.
"""

from collections import Counter
from typing import Any

import progress.bar as progress
from mypy_boto3_dynamodb.service_resource import Table

from aws.authentication.generate_aws_resource import get_session_for_stage
from enums.enums import Stage

STAGE = Stage.HOUSING_STAGING
TABLE_NAME = "Assets"
PROJECTION_EXPRESSION = "assetType"
MISSING_ASSET_TYPE = "<missing>"


def get_asset_type_counts(asset_table: Table) -> Counter[str]:
    scan_kwargs: dict[str, Any] = {"ProjectionExpression": PROJECTION_EXPRESSION}
    counts: Counter[str] = Counter()

    # item_count is an approximate count, used only to size the progress bar
    with progress.Bar(
        f"Scanning {TABLE_NAME} table", max=max(asset_table.item_count, 1)
    ) as progress_bar:
        while True:
            response = asset_table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                asset_type = item.get("assetType")
                counts[asset_type if asset_type else MISSING_ASSET_TYPE] += 1
            progress_bar.next(response.get("ScannedCount", 0))

            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return counts


def print_report(counts: Counter[str]) -> None:
    asset_type_width = 40
    count_width = 8
    separator = "-" * (asset_type_width + count_width)

    print(f"\nAsset type counts for {TABLE_NAME} ({STAGE.value})")
    print(f"{'assetType':<{asset_type_width}}{'count':>{count_width}}")
    print(separator)
    for asset_type, count in counts.most_common():
        print(f"{asset_type:<{asset_type_width}}{count:>{count_width}}")
    print(separator)
    print(f"{'Total':<{asset_type_width}}{sum(counts.values()):>{count_width}}")


def main() -> None:
    session = get_session_for_stage(STAGE)
    asset_table: Table = session.resource("dynamodb").Table(TABLE_NAME)

    print_report(get_asset_type_counts(asset_table))


if __name__ == "__main__":
    main()
