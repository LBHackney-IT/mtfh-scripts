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

STAGE = Stage.HOUSING_PRODUCTION
FILE_PATH = "data/assets_to_load.tsv"
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


def convert_json_str_bool_to_python_bool(d: dict) -> dict:
    """Recursively replace string 'true' with boolean True, and 'false' with False."""
    return {
        k: (
            convert_json_str_bool_to_python_bool(v)
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
    # 1. Write directly to DynamoDB (id already resolved in generate_assets_json) (strip Nones so they're absent, not DynamoDBNull)
    stripped_asset = strip_none_values(asset)
    stripped_asset = convert_json_str_bool_to_python_bool(stripped_asset)
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
        "sourceDomain": "new-properties-load-script-2026",
        "sourceSystem": "new-properties-load-script-2026",
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


def generate_assets_json() -> list[dict]:
    asset_csv_data = csv_to_dict_list(FILE_PATH, is_tsv=True)
    asset_csv_data = [row for row in asset_csv_data if row.get("Property Reference")]

    # Step 1: Resolve UUIDs — reuse existing IDs if asset already in DynamoDB
    asset_id_map: dict[str, str] = {}
    for item in asset_csv_data:
        prop_ref = str(item["Property Reference"])
        existing_assets = get_by_secondary_index(
            table=asset_table,
            index_name="AssetId",
            secondary_key_name="assetId",
            secondary_key_value=prop_ref,
        )
        if existing_assets:
            assert (
                len(existing_assets) == 1
            ), f"Found {len(existing_assets)} assets with assetId {prop_ref}"
            asset_id_map[prop_ref] = existing_assets[0]["id"]
        else:
            asset_id_map[prop_ref] = str(uuid.uuid4())

    def get_floor_number(floor_str: str | None) -> str | None:
        """Parse floor description like '1st floor', 'Gnd floor' into '1', '0'."""
        if not floor_str:
            return None
        floor_str = floor_str.strip().lower()
        if floor_str.startswith("gnd") or floor_str.startswith("ground"):
            return "0"
        digits = ""
        for ch in floor_str:
            if ch.isdigit():
                digits += ch
            elif digits:
                break
        return digits or None

    def get_beds(beds_str: int | None) -> int | None:
        if beds_str is None:
            return None
        try:
            return int(beds_str)
        except (ValueError, TypeError):
            return None

    # Step 2: Build assets from TSV rows

    column_maps = {
        "assetId": "Property Reference",
        "addressLine1": "Address Line 1",
        "addressLine2": "Address Line 2",
        "addressLine3": "Estate Address",
        "addressLine4": "Address Line 4",
        "postCode": "Postcode",
        "postPreamble": None,
        "uprn": "UPRN",
        "floorNo": "Floor",
        "numberOfBedrooms": "Number of Bedrooms",
    }

    assets: list[Asset] = [
        Asset(
            id=asset_id_map[str(item["Property Reference"])],
            assetId=str(item["Property Reference"]),
            assetType="Dwelling",
            assetAddress=AssetAddress.from_data(
                {
                    "addressLine1": item.get(column_maps["addressLine1"]) or None,
                    "addressLine2": item.get(column_maps["addressLine2"]) or None,
                    "addressLine3": item.get(column_maps["addressLine3"]) or None,
                    "addressLine4": item.get(column_maps["addressLine4"]) or None,
                    "postCode": item.get(column_maps["postCode"]) or None,
                    "postPreamble": None,
                    "uprn": str(item.get(column_maps["uprn"])) or None,
                }
            ),
            assetLocation={
                "floorNo": get_floor_number(item.get(column_maps["floorNo"])),
                "totalBlockFloors": None,
                "parentAssets": [],
            },
            assetCharacteristics={
                "yearConstructed": "",
                "numberOfFloors": None,
                "numberOfLifts": None,
                "numberOfBedrooms": get_beds(item.get(column_maps["numberOfBedrooms"])),
                "numberOfSingleBeds": None,
                "numberOfDoubleBeds": None,
                "numberOfBedSpaces": None,
                "hasRampAccess": None,
                "heating": "",
            },
            tenure=None,
            areaId=None,
            patchId=None,
            rentGroup="HRA",
            isActive=None,
            rootAsset="ROOT",
            parentAssetIds=None,
            assetManagement={"owner": "LBH", "isCouncilProperty": True},
        )
        for item in asset_csv_data
    ]

    assets_dicts = [asdict(asset) for asset in assets]

    return assets_dicts


def check_assets_created(assets: list[Asset]):
    with progress.Bar("Checking assets", max=len(assets)) as pbar:
        for asset in assets:
            # Asset API
            response = requests.get(
                f"{asset_url}/assets/assetId/{asset.assetId}",
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


def create_asset_search_result_from_asset_id(asset_id: str) -> dict:
    """Fetch an asset from the Asset API by its asset ID (property reference),
    convert to the asset search result format, and index it in Elasticsearch.

    Returns the Elasticsearch document body that was indexed.
    """
    # Fetch asset from Asset API by assetId
    response = requests.get(
        f"{asset_url}/assets/assetId/{asset_id}",
        headers={"Authorization": f"Bearer {hackney_jwt}"},
    )
    response.raise_for_status()
    asset = response.json()

    # Remove existing search results for this asset ID if any
    matching_assets = search_asset_by_asset_id(asset_id)
    for match in matching_assets:
        elasticsearch_client.delete(doc_id=match["id"])

    # Strip None values so Elasticsearch doesn't store nulls unnecessarily
    stripped_asset = strip_none_values(asset)
    stripped_asset = convert_json_str_bool_to_python_bool(stripped_asset)

    # Index the asset into the Elasticsearch assets index
    elasticsearch_client.index(doc_id=stripped_asset["id"], body=stripped_asset)

    return stripped_asset


def create_search_results_for_all_assets():
    """Fetch all assets from the Asset API and index them in Elasticsearch."""
    with open(ASSETS_LOAD_FILE, "r") as f:
        assets_dicts = json.load(f)

    with progress.Bar(
        "Creating search results for assets", max=len(assets_dicts)
    ) as pbar:
        for asset in assets_dicts:
            create_asset_search_result_from_asset_id(asset["assetId"])
            pbar.next()


def main():
    # 1. Generate JSON from TSV for manual inspection
    assets_dicts_o = generate_assets_json()

    with open(ASSETS_LOAD_FILE, "w") as f:
        json.dump(assets_dicts_o, f, indent=2)
    with open(ASSETS_LOAD_FILE, "r") as f:
        assets_dicts = json.load(f)

    # 2. Load assets into DynamoDB and emit events
    # assets_dicts = assets_dicts[0:1]  # Limit for testing
    with progress.Bar("Uploading assets", max=len(assets_dicts)) as progress_bar:
        for asset in assets_dicts:
            if create_asset_dynamo(asset):
                with open("assets_loaded.txt", "a") as f:
                    f.write(f"{asset['assetId']}\n")
            progress_bar.next()

    # 3. Check assets are valid in Asset API and Search API
    # check_assets_created([Asset(**asset_dict) for asset_dict in assets_dicts])


if __name__ == "__main__":
    create_search_results_for_all_assets()
