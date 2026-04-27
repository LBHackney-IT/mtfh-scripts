import json
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

import progress.bar as progress
import requests
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.database.domain.dynamo_domain_objects import Asset, AssetAddress
from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.opensearch.client.elasticsearch_client import LocalElasticsearchClient
from aws.utils.csv_to_dict_list import csv_to_dict_list
from enums.enums import Stage


STAGE = Stage.HOUSING_DEVELOPMENT
FILE_PATH = "data/new_builds_hierarchy.tsv"
ASSETS_LOAD_FILE = "assets_to_load.json"


session = get_session_for_stage(STAGE)

# DynamoDB table for Assets
asset_table: Table = session.resource("dynamodb").Table("Assets")

# SSM client for fetching configuration like API URLs and SNS topic ARNs
ssm_client: SSMClient = session.client("ssm")

# Asset API
path_asset_api = f"/housing-tl/{STAGE.to_env_name()}/property-api-url"
asset_url = ssm_client.get_parameter(Name=path_asset_api)["Parameter"].get("Value")
assert asset_url, "Asset API URL not found in SSM"

# Search API
path_search_url = f"/housing-tl/{STAGE.to_env_name()}/search-api-url"
search_url = ssm_client.get_parameter(Name=path_search_url)["Parameter"].get("Value")
assert search_url, "Search API URL not found in SSM"

# Hackney JWT for authenticating to APIs
hackney_jwt = os.environ.get("HACKNEY_JWT")
assert hackney_jwt, "HACKNEY_JWT environment variable not set"

# Local Elasticsearch client for directly manipulating search index
elasticsearch_client = LocalElasticsearchClient(index="assets", port=9200)


def search_asset_by_asset_id(asset_id: str) -> list[dict]:
    response = requests.get(
        f"{search_url}/search/assets?searchText={asset_id}".replace("v1/", "v2/"),
        headers={"Authorization": f"Bearer {hackney_jwt}"},
    )
    response.raise_for_status()
    results = response.json().get("results", {}).get("assets", [])
    matches = [result for result in results if result.get("assetId") == asset_id]
    return matches


def strip_none_values(d: dict) -> dict:
    """Recursively remove keys with None values so DynamoDB doesn't store DynamoDBNull,
    which the .NET SDK cannot deserialise into Guid? or other nullable types."""
    return {
        k: strip_none_values(v) if isinstance(v, dict) else v
        for k, v in d.items()
        if v is not None
    }


def replace_true_with_bool(d: dict) -> dict:
    """Recursively replace string 'true' with boolean True, and 'false' with False."""
    return {
        k: (
            replace_true_with_bool(v)
            if isinstance(v, dict)
            else (
                (True if v.lower() == "true" else False if v.lower() == "false" else v)
                if isinstance(v, str)
                else v
            )
        )
        for k, v in d.items()
    }


def create_asset_dynamo(asset: dict) -> bool:
    # 1. Check if asset already exists by assetId
    assert asset["assetId"], "Asset must have an assetId"
    existing_assets = get_by_secondary_index(
        asset_table, "AssetId", "assetId", asset["assetId"]
    )
    if existing_assets:
        # put item at that id instead
        assert (
            len(existing_assets) == 1
        ), f"Existing {len(existing_assets)} assets found with assetId {asset['assetId']}"
        existing_asset = existing_assets[0]
        asset["id"] = existing_asset["id"]

    # 2. Write directly to DynamoDB (strip Nones so they're absent, not DynamoDBNull)
    stripped_asset = strip_none_values(asset)
    stripped_asset = replace_true_with_bool(stripped_asset)
    asset_table.put_item(Item=stripped_asset)

    # Fetch asset from asset API to verify schema validation
    response = requests.get(
        f"{asset_url}/assets/{asset['id']}",
        headers={"Authorization": f"Bearer {hackney_jwt}"},
    )
    response.raise_for_status()
    fetched_asset = response.json()

    assert fetched_asset["assetId"] == asset["assetId"]

    # 3. Emit AssetCreatedEvent to SNS if needed
    # Remove existing search results if they exist
    matching_assets = search_asset_by_asset_id(asset["assetId"])
    if len(matching_assets) >= 0:  # if any assets found, delete so event re-creates it
        for match in matching_assets:
            elasticsearch_client.delete(doc_id=match["id"])
    emit_asset_created_event(stripped_asset)  # Create in search

    return True


def create_sns_message(
    event_type: str,
    user: dict,
    entity_id: str,
    event_data: dict,
) -> dict:
    return {
        "id": entity_id,
        "eventType": event_type,
        "sourceDomain": "mtfh-api",
        "sourceSystem": "mtfh-api",
        "version": "v1",
        "correlationId": str(uuid.uuid4()),
        "dateTime": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "entityId": entity_id,
        "eventData": event_data,
    }


def emit_asset_created_event(asset: dict):
    sns_client = session.client("sns")
    topic_arn = ssm_client.get_parameter(
        Name=f"/sns-topic/{STAGE.to_env_name()}/asset/arn"
    )["Parameter"].get("Value")
    assert topic_arn, "Asset Created SNS Topic ARN not found in SSM"
    message = create_sns_message(
        event_type="AssetCreatedEvent",
        user={"name": "Adam", "email": "adam.tracy@hackney.gov.uk"},
        entity_id=asset["id"],
        event_data={
            "oldData": None,
            "newData": asset,
        },
    )
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps({"default": json.dumps(message)}),
        MessageStructure="json",
        MessageGroupId="fake",
    )


def generate_assets_json():
    asset_csv_data = csv_to_dict_list(FILE_PATH, is_tsv=True)

    # Step 1: Generate UUIDs for all assets keyed by prop_ref
    asset_id_map: dict[str, str] = {
        str(item["assetId"]): str(uuid.uuid4()) for item in asset_csv_data
    }

    asset_id_map["00078658"] = "e7cf983e-c112-15a6-8717-c4d736aaadbd"  # Existing estate

    # Build a lookup map of assetId -> row for resolving parent details
    asset_data_map: dict[str, dict] = {
        str(item["assetId"]): item for item in asset_csv_data
    }

    # Step 2: Build assets, resolving parentAssetIds from prop_ref -> UUID
    assets: list[Asset] = [
        Asset(
            id=asset_id_map[str(item["assetId"])],
            assetId=str(item["assetId"]),
            assetType=item.get("assetType"),
            assetAddress=AssetAddress.from_data(
                {
                    "postCode": item.get("assetAddress.postCode") or "N1 7UQ",
                    "postPreamble": item.get("assetAddress.postPreamble") or None,
                    "addressLine1": item.get("assetAddress.addressLine1") or None,
                    "uprn": str(item.get("assetAddress.uprn")) or None,
                }
            ),
            assetLocation={
                "floorNumber": item.get("assetLocation.floorNumber") or None,
                "totalBlockFloors": item.get("assetLocation.totalBlockFloors") or None,
                "parentAssets": (
                    [
                        {
                            "id": asset_id_map.get(
                                str(item["parentAssetIds (need to convert to UUID)"])
                            ),
                            "type": asset_data_map.get(
                                str(item["parentAssetIds (need to convert to UUID)"]),
                                {},
                            ).get("assetType"),
                            "name": asset_data_map.get(
                                str(item["parentAssetIds (need to convert to UUID)"]),
                                {},
                            ).get("assetAddress.addressLine1"),
                        }
                    ]
                    if item.get("parentAssetIds (need to convert to UUID)")
                    else []
                ),
            },
            assetCharacteristics={
                "yearConstructed": str(
                    item.get("assetCharacteristics.yearConstructed")
                ),
                "numberOfFloors": item.get("assetCharacteristics.numberOfFloors")
                or None,
                "numberOfLifts": (
                    int(item["assetCharacteristics.numberOfLifts"])
                    if item.get("assetCharacteristics.numberOfLifts")
                    else None
                ),
                "numberOfBedrooms": (
                    int(item["assetCharacteristics.numberOfBedrooms"])
                    if item.get("assetCharacteristics.numberOfBedrooms")
                    else None
                ),
                "numberOfSingleBeds": (
                    int(item["assetCharacteristics.numberOfSingleBeds"])
                    if item.get("assetCharacteristics.numberOfSingleBeds")
                    else None
                ),
                "numberOfDoubleBeds": (
                    int(item["assetCharacteristics.numberOfDoubleBeds"])
                    if item.get("assetCharacteristics.numberOfDoubleBeds")
                    else None
                ),
                "numberOfBedSpaces": (
                    int(item["assetCharacteristics.numberOfBedSpaces"])
                    if item.get("assetCharacteristics.numberOfBedSpaces")
                    else None
                ),
                "hasRampAccess": item.get("assetCharacteristics.hasRampAccess") or None,
                "heating": item.get("assetCharacteristics.heating"),
            },
            tenure=None,
            areaId=None,
            patchId=None,
            rentGroup=None,
            isActive=None,
            rootAsset=(
                asset_id_map["00078658"] if item.get("assetType") != "Block" else None
            ),
            parentAssetIds=(
                asset_id_map.get(str(item["parentAssetIds (need to convert to UUID)"]))
                if item.get("parentAssetIds (need to convert to UUID)")
                else None
            ),
            assetManagement={"owner": "LBH"},
        )
        for item in asset_csv_data
    ]

    assets_dicts = [asdict(asset) for asset in assets]

    for asset_dict in assets_dicts:
        if asset_dict["rootAsset"] is None:
            asset_dict["rootAsset"] = "ROOT"

    with open(ASSETS_LOAD_FILE, "w") as f:
        json.dump(assets_dicts, f, indent=4)


def check_assets_created(assets: list[Asset]):
    with progress.Bar("Checking assets", max=len(assets)) as pbar:
        for asset in assets:
            # Asset API
            response = requests.get(
                f"{asset_url}/assets/{asset.id}",
                headers={"Authorization": f"Bearer {hackney_jwt}"},
            )
            if response.status_code != 200:
                print(f"Asset {asset.assetId} not found in Asset API")

            # Search API
            search_response = requests.get(
                f"{search_url}/search/assets?searchText={asset.assetId}".replace(
                    "v1/", "v2/"
                ),
                headers={"Authorization": f"Bearer {hackney_jwt}"},
            )
            if search_response.status_code != 200:
                print(f"Asset {asset.assetId} not found in Search API")
            results = search_response.json().get("results", {}).get("assets", [])
            matching_results = [
                result for result in results if result.get("assetId") == asset.assetId
            ]
            if len(matching_results) == 0:
                print(f"Asset {asset.assetId} not found in Search results")
            elif len(matching_results) > 1:
                print(
                    f"{len(matching_results)} assets with assetId "
                    f"{asset.assetId} found in Search results"
                )
            pbar.next()


def main():
    # 1. Generate JSON from TSV for manual inspection
    # generate_assets_json()

    with open(ASSETS_LOAD_FILE, "r") as f:
        assets_dicts = json.load(f)

    # 2. Load assets into DynamoDB and emit events
    # assets_dicts = assets_dicts[1:10]  # Limit for testing
    # with progress.Bar("Uploading assets", max=len(assets_dicts)) as progress_bar:
    #     for asset in assets_dicts:
    #         if create_asset_dynamo(asset):
    #             with open("assets_loaded.txt", "a") as f:
    #                 f.write(f"{asset['assetId']}\n")
    #         progress_bar.next()

    # 3. Check assets are valid in Asset API and Search API
    check_assets_created([Asset(**asset_dict) for asset_dict in assets_dicts])


if __name__ == "__main__":
    main()
