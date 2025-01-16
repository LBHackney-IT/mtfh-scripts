"""
This script updates asset tenures in a DynamoDB table and an Elasticsearch index based on data from a TSV file.
It fetches current and new tenures for assets, generates fixups, and applies the updates to both DynamoDB and Elasticsearch.
The script also verifies the updates by checking the asset and search APIs.

Requirements:
- Port forwarding to the housing search Elasticsearch on the specified port (ES_PORT) - see Makefile
- AWS CLI profile with the necessary permissions

Key functions:
- main(): The main function that orchestrates the script
- generate_fixups(): Generates fixups (change previews) based on the data in the TSV file
- update_asset_tenures_from_fixups(): Updates asset tenures in DynamoDB based on the fixups
- update_elasticsearch_from_fixups(): Updates asset tenures in Elasticsearch based on the fixups
- verify_work_complete(): Verifies that the updates have been correctly applied
"""

from datetime import date
from typing import TypedDict, cast

from boto3 import Session
from mypy_boto3_ssm import SSMClient
from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from enums.enums import Stage
import os
import requests
import json

from aws.database.opensearch.client.elasticsearch_client import create_client
from utils.confirm import confirm


ES_PORT = 9200
_cwd = os.path.dirname(os.path.realpath(__file__))
FILE_DIR = f"{_cwd}/data"


class FileRow(TypedDict):
    """Represents a row in the TSV file"""

    property_ref: str
    tenure_description: str
    payment_ref: str
    full_name: str
    full_address: str
    start_date: str
    end_date: str
    buy_back_date: str


class AssetTenure(TypedDict):
    """Represents an asset.tenure object"""

    id: str
    paymentReference: str
    type: str
    startOfTenureDate: str
    endOfTenureDate: str | None


class Fixup(TypedDict):
    """Represents changes to be made to an asset's tenure"""

    prop_id: str
    prop_ref: str
    address: str
    current_tenure: AssetTenure
    new_tenures: list[AssetTenure]


def tsv_to_file_rows(file_path: str) -> list[FileRow]:
    file_path = os.path.join(FILE_DIR, file_path)
    with open(file_path, "r") as file:
        lines = file.readlines()
        headers = lines[0].strip().split("\t")
        data = [line.strip().split("\t") for line in lines[1:]]

    dicts = [dict(zip(headers, row)) for row in data]
    file_rows = [FileRow(**row) for row in dicts]
    return file_rows


def fetch_ssm_parameter(client: SSMClient, parameter_name: str) -> str:
    response = client.get_parameter(Name=parameter_name, WithDecryption=True)
    assert "Parameter" in response, response
    assert "Value" in response["Parameter"], response["Parameter"]
    return response["Parameter"]["Value"]


def get_search_api_url(stage: Stage, ssm: SSMClient) -> str:
    search_api_url = fetch_ssm_parameter(
        ssm, f"/housing-tl/{stage.to_env_name()}/search-api-url"
    )
    search_api_url = search_api_url.replace("v1", "v2/search")
    return search_api_url


def get_asset_api_url(stage: Stage, ssm: SSMClient) -> str:
    asset_api_url = fetch_ssm_parameter(
        ssm, f"/housing-tl/{stage.to_env_name()}/asset-api-url"
    )
    return asset_api_url


def get_asset_from_asset_api(asset_api_url: str, asset_id: str) -> dict:
    headers = {"Authorization": os.environ["HACKNEY_JWT"]}
    response = requests.get(f"{asset_api_url}assets/{asset_id}", headers=headers)
    response.raise_for_status()
    return response.json()


def get_asset_from_search_api(search_api_url: str, asset_id: str) -> dict:
    headers = {"Authorization": os.environ["HACKNEY_JWT"]}
    response = requests.get(
        f"{search_api_url}/assets?searchText={asset_id}", headers=headers
    )
    response.raise_for_status()
    assets_res = response.json()["results"]["assets"]
    asset = [asset for asset in assets_res if asset["id"] == asset_id][0]
    return asset


def tenures_for_asset(search_api_url: str, asset_id: str) -> list[dict]:
    """Fetches all tenures linked to a property from the search API, sorted by start date"""
    headers = {"Authorization": os.environ["HACKNEY_JWT"]}
    response = requests.get(
        f"{search_api_url}/tenures?searchText={asset_id}",
        headers=headers,
    )
    response.raise_for_status()
    tenures = response.json()["results"]["tenures"]
    tenures_at_property = [
        tenure for tenure in tenures if tenure["tenuredAsset"]["id"] == asset_id
    ]
    assert len(tenures_at_property) > 1, tenures_at_property

    # sort by parsed start date
    def tenure_start_date(tenure: dict) -> date:
        start_date = tenure["startOfTenureDate"].split("T")[0]
        return date.fromisoformat(start_date)

    sorted_tenures = sorted(tenures_at_property, key=tenure_start_date, reverse=True)

    return sorted_tenures


def generate_fixups(stage: Stage, session: Session) -> list[Fixup]:
    # Initialize resources
    ssm: SSMClient = session.client("ssm")
    search_api_url = get_search_api_url(stage, ssm)

    dynamodb = session.resource("dynamodb")
    assets_table = dynamodb.Table("Assets")
    tenures_table = dynamodb.Table("TenureInformation")

    tenancies_to_fix = tsv_to_file_rows("tenancies_to_fix.tsv")

    fixups: list[Fixup] = []

    for sheet_tenancy in tenancies_to_fix:
        print(f"Getting tenancy for property: {sheet_tenancy['property_ref']}")

        # - Get current asset assigned to tenure
        assets = get_by_secondary_index(
            assets_table, "AssetId", "assetId", sheet_tenancy["property_ref"]
        )
        assert len(assets) == 1, assets
        asset = assets[0]
        assert "tenure" in asset, asset

        current_asset_tenure: AssetTenure = {
            "id": asset["tenure"]["id"],
            "paymentReference": asset["tenure"]["paymentReference"],
            "type": asset["tenure"]["type"],
            "startOfTenureDate": asset["tenure"]["startOfTenureDate"],
            "endOfTenureDate": asset["tenure"].get("endOfTenureDate"),
        }
        print("Successfully collected current asset tenure")

        # - Get new tenure that should be assigned to the asset

        # Must use search API because we have no other way to get tenures by prop ref
        print("Fetching new tenure from search API")
        asset_tenures = tenures_for_asset(search_api_url, asset["id"])

        # Replace search API tenures with tenure API tenures (more reliable data)
        new_asset_tenures = []
        for new_asset_active_tenure in asset_tenures:
            print(f"Getting new tenure from tenure ID: {new_asset_active_tenure['id']}")
            new_tenure = tenures_table.get_item(
                Key={"id": new_asset_active_tenure["id"]}
            ).get("Item")
            assert new_tenure is not None, new_tenure

            new_asset_tenure: AssetTenure = {
                "id": str(new_tenure["id"]),
                "paymentReference": str(new_tenure["paymentReference"]),
                "type": str(cast(dict, new_tenure["tenureType"])["description"]),
                "startOfTenureDate": str(new_tenure["startOfTenureDate"]),
                "endOfTenureDate": cast(str | None, new_tenure.get("endOfTenureDate")),
            }
            print(f"Found new tenure: {new_asset_tenure}")
            new_asset_tenures.append(new_asset_tenure)

        fixup: Fixup = {
            "prop_id": asset["id"],
            "prop_ref": sheet_tenancy["property_ref"],
            "address": sheet_tenancy["full_address"],
            "current_tenure": {
                "id": current_asset_tenure["id"],
                "paymentReference": current_asset_tenure["paymentReference"],
                "type": current_asset_tenure["type"],
                "startOfTenureDate": current_asset_tenure["startOfTenureDate"],
                "endOfTenureDate": current_asset_tenure["endOfTenureDate"],
            },
            "new_tenures": [
                {
                    "id": new_asset_tenure["id"],
                    "paymentReference": new_asset_tenure["paymentReference"],
                    "type": new_asset_tenure["type"],
                    "startOfTenureDate": new_asset_tenure["startOfTenureDate"],
                    "endOfTenureDate": new_asset_tenure["endOfTenureDate"],
                }
                for new_asset_tenure in new_asset_tenures
            ],
        }
        fixups.append(fixup)

    return fixups


def update_asset_tenures_from_fixups(session: Session, fixups: list[Fixup]) -> None:
    asset_table = session.resource("dynamodb").Table("Assets")

    for fixup in fixups:
        current_asset = cast(
            dict, asset_table.get_item(Key={"id": fixup["prop_id"]}).get("Item")
        )
        assert current_asset is not None, current_asset

        # The first is the most recent tenure
        proposed_new_tenure = fixup["new_tenures"][0]

        current_tenure_id = current_asset.get("tenure", {}).get("id")
        assert current_tenure_id is not None, current_asset
        if current_tenure_id == proposed_new_tenure["id"]:
            print(
                f"Property {fixup['prop_id']} already has the correct tenure assigned"
            )
            continue

        if confirm(
            f"Update tenure for property {fixup['prop_id']} from \n"
            f"{json.dumps(fixup['current_tenure'], indent=4)} -> \n"
            f"{json.dumps(proposed_new_tenure, indent=4)}?"
        ):
            asset_table.update_item(
                Key={"id": fixup["prop_id"]},
                UpdateExpression="SET tenure = :new_tenure",
                ExpressionAttributeValues={":new_tenure": fixup["new_tenures"][0]},
            )
        else:
            print(f"Skipping property {fixup['prop_id']}")


def update_elasticsearch_from_fixups(fixups: list[Fixup]) -> None:
    index_ = "assets"
    es_client = create_client(index_, ES_PORT)

    for fixup in fixups:
        # The first is the most recent tenure
        proposed_new_tenure = fixup["new_tenures"][0]

        current_asset = cast(dict, es_client.get(index_, fixup["prop_id"]))["_source"]
        assert current_asset["id"] == fixup["prop_id"], current_asset

        # if the tenure ids are the same, skip
        current_tenure_id = current_asset.get("tenure", {}).get("id")
        assert current_tenure_id is not None, current_asset
        if current_tenure_id == proposed_new_tenure["id"]:
            print(
                f"Property {fixup['prop_id']} already has the correct tenure assigned"
            )
            continue

        if confirm(
            f"Update asset search tenure for property {fixup['prop_id']} from \n"
            f"{json.dumps(current_asset.get("tenure", {}), indent=4)} -> \n"
            f"{json.dumps(proposed_new_tenure, indent=4)}?"
        ):
            es_client.update(
                index_, fixup["prop_id"], body={"doc": {"tenure": proposed_new_tenure}}
            )


def verify_work_complete(stage: Stage, fixups: list[Fixup]) -> None:
    session = get_session_for_stage(stage)
    ssm = session.client("ssm")
    asset_api_url = get_asset_api_url(stage, ssm)
    search_api_url = get_search_api_url(stage, ssm)

    for fixup in reversed(fixups):
        print(f"Verifying property {fixup['prop_id']}")
        expected_tenure = fixup["new_tenures"][0]

        # Check asset API
        api_asset = get_asset_from_asset_api(asset_api_url, fixup["prop_id"])
        assert api_asset["tenure"]["id"] == expected_tenure["id"], api_asset
        assert expected_tenure["type"] in api_asset["tenure"]["type"], api_asset

        # Check search API
        search_asset = get_asset_from_search_api(search_api_url, fixup["prop_id"])
        assert search_asset["tenure"]["id"] == expected_tenure["id"], search_asset
        assert expected_tenure["type"] in search_asset["tenure"]["type"], search_asset

        try:
            assert "leasehold" not in api_asset["tenure"]["type"].lower(), api_asset
            assert (
                "leasehold" not in search_asset["tenure"]["type"].lower()
            ), search_asset
        except AssertionError:
            print(
                f"WARNING: Property {fixup['prop_id']} - {fixup['prop_ref']} has a leasehold"
            )

        print(f"Property {fixup['prop_id']} has been updated correctly")


def main():
    stage = Stage.HOUSING_DEVELOPMENT
    session = get_session_for_stage(stage)

    # Part 1: Generate fixups
    fixups = generate_fixups(stage, session)
    with open(os.path.join(FILE_DIR, "fixups.json"), "w") as file:
        json.dump(fixups, file, indent=4)

    if not confirm("After reviewing the fixups.json file, continue?"):
        return
    # You can quit the script here to take time to review the fixups.json file - it will be saved in the FIXUP_FILE_DIR

    # Part 2: Update asset tenures in DynamoDB and Elasticsearch after manual fixup review
    with open(os.path.join(FILE_DIR, "fixups.json"), "r") as file:
        fixups = cast(list[Fixup], json.load(file))
    update_asset_tenures_from_fixups(session, fixups)
    if confirm(
        f"Ensure you are port forwarding to the housing search elasticsearch on {ES_PORT}. "
        "Update Elasticsearch with the same changes?"
    ):
        update_elasticsearch_from_fixups(fixups)

    # Part 3: Verify that the updates have been correctly applied
    verify_work_complete(stage, fixups)


if __name__ == "__main__":
    main()
