"""
- Remove account 1907785902 from tenure table
- Re-point asset for account 1907785902 to account 6803061408
- Re-point person for account 1907785902 to account 6803061408

Manual steps:
- Delete account 1907785902 from tenure elasticsearch index
- Re-point asset for account 1907785902 to account 6803061408 in asset elasticsearch index
- Re-point person for account 1907785902 to account 6803061408 in person elasticsearch index
"""

from dataclasses import asdict
from typing import cast

from mypy_boto3_dynamodb import DynamoDBServiceResource

from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.domain.dynamo_domain_objects import AssetTenure, PersonTenure, Tenure
from enums.enums import Stage
from utils.confirm import confirm

STAGE = Stage.HOUSING_PRODUCTION
TENURE_ID = "9ab9a6c6-3f7b-4e02-83ec-46a3d48ab7c7"  # 1907785902
# TENURE_ID = "a464053b-1449-15ff-da39-4f2b441f1a6f"  # staging
REPOINT_TENURE_ID = "66efdf04-9ad7-475a-ad15-0db081a1826f"  # 6803061408

PERSON_ID = "12835183-40d9-4e87-adfc-d40c0f4e5e3b"  # 1907785902
ASSET_ID = "016e31a8-c4aa-b253-6783-d2dc8f7453e1"  # 1907785902


dynamodb: DynamoDBServiceResource = generate_aws_service("dynamodb", STAGE)

assets_table = dynamodb.Table("Assets")
tenure_table = dynamodb.Table("TenureInformation")
person_table = dynamodb.Table("Persons")


def set_asset_tenure(asset_id: str, tenure_id: str):
    """Set the tenure of an asset based on the tenure ID"""
    tenure = tenure_table.get_item(Key={"id": tenure_id}).get("Item")
    assert tenure is not None, f"Tenure with ID {tenure_id} not found"

    asset_tenure = AssetTenure(
        id=str(tenure["id"]),
        paymentReference=str(tenure["paymentReference"]),
        startOfTenureDate=str(tenure["startDate"]),
        endOfTenureDate=str(tenure["endDate"]) if tenure.get("endDate") else None,
        type=str(tenure["tenureType"]),
    )

    if not confirm(f"{asset_id}.tenure -> {asset_tenure.__dict__}?"):
        print("Aborting")
        return

    assets_table.update_item(
        Key={"id": asset_id},
        UpdateExpression="SET tenure = :tenure",
        ExpressionAttributeValues={":tenure": asdict(asset_tenure)},
    )
    print(f"Updated asset {asset_id} to point to tenure {tenure_id}")


def set_person_tenure(person_id: str, old_tenure_id: str, new_tenure_id: str):
    """Set the tenure of a person based on the tenure ID"""
    person = person_table.get_item(Key={"id": person_id}).get("Item")
    assert person is not None, f"Person with ID {person_id} not found"

    tenures = cast(list[dict], person.get("tenures", []))
    matching_tenure = [tenure for tenure in tenures if tenure["id"] == old_tenure_id][0]
    tenure_index = tenures.index(matching_tenure)

    new_tenure_raw = tenure_table.get_item(Key={"id": new_tenure_id}).get("Item")
    assert new_tenure_raw is not None, f"Tenure with ID {new_tenure_id} not found"
    new_tenure = cast(Tenure, Tenure.from_data(new_tenure_raw))

    new_person_tenure = PersonTenure(
        id=new_tenure.id,
        assetId=new_tenure.tenuredAsset.id if new_tenure.tenuredAsset else None,
        propertyReference=(
            new_tenure.tenuredAsset.propertyReference
            if new_tenure.tenuredAsset
            else None
        ),
        assetFullAddress=(
            new_tenure.tenuredAsset.fullAddress if new_tenure.tenuredAsset else None
        ),
        paymentReference=new_tenure.paymentReference,
        startDate=new_tenure.startOfTenureDate,
        endDate=new_tenure.endOfTenureDate,
        type=(
            new_tenure.tenureType.get("description") if new_tenure.tenureType else None
        ),
        uprn=new_tenure.tenuredAsset.uprn if new_tenure.tenuredAsset else None,
    )

    if not confirm(
        f"{person_id}.tenures[{tenure_index}].id -> {asdict(new_person_tenure)}?"
    ):
        print("Aborting")
        return

    person_table.update_item(
        Key={"id": person_id},
        UpdateExpression=f"SET tenures[{tenure_index}].id = :new_tenure_id",
        ExpressionAttributeValues={":new_tenure_id": new_tenure_id},
    )
    print(f"Updated person {person_id} to point to tenure {new_tenure_id}")


def delete_tenure(tenure_id: str):
    if not confirm(f"Delete tenure {tenure_id}?"):
        print("Aborting")
        return

    tenure_table.delete_item(Key={"id": tenure_id})
    print(f"Deleted tenure {tenure_id}")


def main():
    repoint_tenure = tenure_table.get_item(Key={"id": REPOINT_TENURE_ID}).get("Item")
    assert repoint_tenure is not None, f"Tenure with ID {REPOINT_TENURE_ID} not found"

    tenure = tenure_table.get_item(Key={"id": TENURE_ID}).get("Item")
    assert tenure is not None, f"Tenure with ID {TENURE_ID} not found"

    asset_id = cast(dict, tenure.get("tenuredAsset", {})).get("id")
    asset = assets_table.get_item(Key={"id": asset_id}).get("Item")
    assert asset is not None, f"Asset with ID {asset_id} not found"

    person = person_table.get_item(Key={"id": PERSON_ID}).get("Item")
    assert person is not None, f"Person with ID {PERSON_ID} not found"

    print(f"Tenure: {TENURE_ID} - {tenure.get('tenuredAsset')}")
    print(
        f"Asset: {asset['id']} - {cast(dict, asset.get('assetAddress', {})).get('addressLine1', 'No address')}"
    )
    print(
        f"Persons: {person['id']} - {str(person.get('firstName', '')) + ' ' + str(person.get('lastName', ''))}"
    )

    asset_tenure = AssetTenure(
        id=str(repoint_tenure["id"]),
        paymentReference=str(repoint_tenure["paymentReference"]),
        startOfTenureDate=str(repoint_tenure["startDate"]),
        endOfTenureDate=(
            str(repoint_tenure["endDate"]) if repoint_tenure.get("endDate") else None
        ),
        type=str(repoint_tenure["tenureType"]),
    )

    # 1. Update asset to point to new tenure
    if asset:
        assets_table.update_item(
            Key={"id": asset["id"]},
            UpdateExpression="SET tenure = :tenure",
            ExpressionAttributeValues={":tenure": asdict(asset_tenure)},
        )
        print(f"Updated asset {asset['id']} to point to tenure {REPOINT_TENURE_ID}")
    # 2. Update persons to point to new tenure
    persons = [person]
    for person in persons:
        matching_tenure = [
            t for t in cast(list, person.get("tenures", [])) if t["id"] == TENURE_ID
        ][0]
        tenure_index = cast(list, person.get("tenures", [])).index(matching_tenure)
        person_table.update_item(
            Key={"id": person["id"]},
            UpdateExpression=f"SET tenures[{tenure_index}].id = :new_tenure_id",
            ExpressionAttributeValues={":new_tenure_id": REPOINT_TENURE_ID},
        )
        print(f"Updated person {person['id']} to point to tenure {REPOINT_TENURE_ID}")
    # 3. Delete old tenure
    tenure_table.delete_item(Key={"id": TENURE_ID})
    print(f"Deleted tenure {TENURE_ID}")


if __name__ == "__main__":
    main()
