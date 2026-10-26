"""
A script to export all assets in the Assets DynamoDB table that are either
marked as being under warranty (assetCharacteristics.isUnderWarranty = true)
or have a new build asset type (assetType in "New Build" / "NewBuild").

The table is scanned with a filter and only the required fields are projected,
then the results are written to a TSV file.
"""

import csv
from typing import Any

import progress.bar as progress
from mypy_boto3_dynamodb.service_resource import Table

from aws.authentication.generate_aws_resource import get_session_for_stage
from enums.enums import Stage

STAGE = Stage.HOUSING_STAGING
TABLE_NAME = "Assets"
OUTPUT_FILE = f"new_build_assets_{STAGE.value}.tsv"

HEADERS = [
    "id",
    "assetId",
    "addressLine1",
    "addressLine2",
    "postCode",
    "underwarranty",
    "newbuild",
]

NEW_BUILD_ASSET_TYPES = ["New Build", "NewBuild"]

PROJECTION_EXPRESSION = (
    "id, assetId, assetAddress.addressLine1, "
    "assetAddress.addressLine2, assetAddress.postCode, "
    "assetCharacteristics.isUnderWarranty, assetType"
)

FILTER_EXPRESSION = (
    "assetCharacteristics.isUnderWarranty = :isUnderWarranty "
    "OR assetType IN (:newBuildType1, :newBuildType2)"
)


def to_row(asset: dict[str, Any]) -> dict[str, str]:
    asset_address = asset.get("assetAddress") or {}
    asset_characteristics = asset.get("assetCharacteristics") or {}
    return {
        "id": asset.get("id") or "",
        "assetId": asset.get("assetId") or "",
        "addressLine1": asset_address.get("addressLine1") or "",
        "addressLine2": asset_address.get("addressLine2") or "",
        "postCode": asset_address.get("postCode") or "",
        "underwarranty": (
            "true" if asset_characteristics.get("isUnderWarranty") else "false"
        ),
        "newbuild": (
            "true" if asset.get("assetType") in NEW_BUILD_ASSET_TYPES else "false"
        ),
    }


def get_assets_under_warranty_or_new_build(
    asset_table: Table,
) -> list[dict[str, str]]:
    scan_kwargs: dict[str, Any] = {
        "ProjectionExpression": PROJECTION_EXPRESSION,
        "FilterExpression": FILTER_EXPRESSION,
        "ExpressionAttributeValues": {
            ":isUnderWarranty": True,
            ":newBuildType1": NEW_BUILD_ASSET_TYPES[0],
            ":newBuildType2": NEW_BUILD_ASSET_TYPES[1],
        },
    }

    assets: list[dict[str, str]] = []
    # item_count is an approximate count, used only to size the progress bar
    with progress.Bar(
        f"Scanning {TABLE_NAME} table", max=max(asset_table.item_count, 1)
    ) as progress_bar:
        while True:
            response = asset_table.scan(**scan_kwargs)
            assets.extend(to_row(item) for item in response.get("Items", []))
            progress_bar.next(response.get("ScannedCount", 0))

            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return assets


def write_tsv(assets: list[dict[str, str]], file_path: str) -> None:
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file, fieldnames=HEADERS, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(assets)


def main() -> None:
    session = get_session_for_stage(STAGE)
    asset_table: Table = session.resource("dynamodb").Table(TABLE_NAME)

    assets = get_assets_under_warranty_or_new_build(asset_table)
    assets.sort(key=lambda asset: asset["id"])
    write_tsv(assets, OUTPUT_FILE)

    print(
        f"Wrote {len(assets)} assets under warranty or of a new build type "
        f"to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
