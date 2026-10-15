from typing import cast

from enums.enums import Stage
from aws.authentication.generate_aws_resource import get_session_for_stage

PROP_REFS = ["prop_ref_1", "prop_ref_2", "etcetera"]


session = get_session_for_stage(Stage.HOUSING_STAGING)
asset_table = session.resource("dynamodb").Table("Assets")


def main():
    for prop_ref in PROP_REFS:
        # Fetch by secondary index assetId
        response = asset_table.query(
            IndexName="AssetId",
            KeyConditionExpression="assetId = :assetId",
            ExpressionAttributeValues={":assetId": prop_ref},
        )

        assets = response.get("Items", [])
        assert len(assets) > 0, f"No items found for prop_ref {prop_ref}"
        assert len(assets) == 1, f"Multiple items found for prop_ref {prop_ref}"
        asset = assets[0]
        existing_characteristics: dict = cast(
            dict, asset.get("assetCharacteristics", {})
        )
        if existing_characteristics.get("isUnderWarranty"):
            print(f"Asset {asset['id']} for prop_ref {prop_ref} already under warranty")
            continue

        asset_table.update_item(
            Key={"id": asset["id"]},
            UpdateExpression="SET assetCharacteristics.isUnderWarranty = :isUnderWarranty",
            ExpressionAttributeValues={":isUnderWarranty": True},
        )

        asset_address: dict = cast(dict, asset.get("assetAddress", {}))
        print(
            f"Updated asset {asset['id']} for prop_ref {prop_ref} "
            f"address {asset_address.get('addressLine1')}"
        )


if __name__ == "__main__":
    main()
