from dataclasses import dataclass, asdict
from dacite import from_dict

import requests
import os

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.authentication.generate_aws_resource import generate_aws_service
from aws.src.database.domain.dynamo_domain_objects import Asset, AssetAddress
from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from enums.enums import Stage

STAGE = Stage.HOUSING_STAGING

INPUT_FILE = "input/input.csv"
FAILED_PRNS_FILE = "output/failed_prns.csv"
SUCCESSFUL_PRNS_FILE = "output/successful_prns.csv"

_ssm = generate_aws_service('ssm', STAGE, "client")
_address_api_url_path = f"/housing-tl/{STAGE.to_env_name()}/address-api-url"
_asset_api_url_path = f"/housing-tl/{STAGE.to_env_name()}/asset-api-url"
ADDRESS_API_URL = _ssm.get_parameter(Name=_address_api_url_path)['Parameter']['Value']
ASSET_API_URL = _ssm.get_parameter(Name=_asset_api_url_path)['Parameter']['Value']


def add_failed_uprn(uprn: int | str, outfile="failed_uprns.csv"):
    with open(outfile, "a") as f:
        f.write(f"{uprn}\n")


def add_successful_uprn(uprn: int | str, outfile="successful_uprns.csv"):
    with open(outfile, "a") as f:
        f.write(f"{uprn}\n")


@dataclass
class AddressAPIRes:
    line1: str
    line2: str
    line3: str
    line4: str
    town: str
    postcode: str
    UPRN: int


def get_asset(asset_table: Table, prop_ref: str) -> Asset | None:
    asset_list: list[dict] = get_by_secondary_index(asset_table, "AssetId", "assetId", prop_ref)
    if len(asset_list) == 0:
        return None
    asset_dict = asset_list[0]
    asset = Asset.from_data(asset_dict)
    return asset


def llpg_address_for_uprn(address_api_url: str, uprn: int | str) -> AddressAPIRes | None:
    """
    Get an address from the LLP Gazetteer via the Address API for a given UPRN
    Requires a "hackneyToken" environment variable to be set to authenticate with the API
    """
    token = os.environ["hackneyToken"]
    if not token:
        raise ValueError("No Hackney token found. Please set the hackneyToken environment variable")

    if str(uprn).isnumeric():
        uprn = int(uprn)
    else:
        add_failed_uprn(uprn)
        return None

    # Step 1: Get address(es) for UPRN
    response = requests.get(
        f"{address_api_url}/addresses",
        params={
            "address_scope": "Hackney Gazetteer",
            "uprn": uprn
        },
        headers={
            "authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
    )
    if response.status_code != 200:
        print(f"WARNING: {response.status_code} response when getting address for UPRN {uprn}")
        add_failed_uprn(uprn)
        return None

    # Step 2: Get first valid address from response (if any) and return it as an Address object
    addresses_list: list[dict] | None = response.json().get("data").get("address")

    try:
        address_json = addresses_list[0]
        assert int(address_json["UPRN"]) == int(uprn)
        address = from_dict(data_class=AddressAPIRes, data=address_json)
        return address
    except (IndexError, KeyError, AssertionError):
        print(f"WARNING: Could not get address for UPRN {uprn}, got: {addresses_list}")
        add_failed_uprn(uprn)
        return None


def update_address_in_dynamodb(asset_table: Table, asset_id: str, uprn: str) -> bool:
    """
    Update the address of an asset in DynamoDB with the address from the Address API
    :param asset_id: The property reference number (aka asset ID) to update
    """

    # Step 1: Get the asset from DynamoDB
    asset = get_asset(asset_table, asset_id)
    if not asset:
        print(f"WARNING: Could not find asset with ID {asset_id}")
        return False

    # Step 2: Get the address from the Address API
    address = llpg_address_for_uprn(ADDRESS_API_URL, uprn)
    if not address:
        return False

    # Step 3: Update the asset in DynamoDB
    asset.assetAddress.addressLine1 = address.line1
    asset.assetAddress.addressLine2 = address.line2
    asset.assetAddress.addressLine3 = address.line3
    asset.assetAddress.addressLine4 = address.line4

    asset_table.put_item(Item=asdict(asset))
    add_successful_uprn(uprn)
    return True


def update_address_asset_api(asset_api_url: str, asset_pk: str, version_number: int, asset_address: AssetAddress):
    """
    Updates an asset's address using a PATCH endpoint in the asset API
    """
    token = os.environ["hackneyToken"]
    if not token:
        raise ValueError("No Hackney token found. Please set the hackneyToken environment variable")
    
    request_json = {
        "assetAddress": asdict(asset_address)
    }

    request_url = f"{asset_api_url}assets/{asset_pk}/address"
    response = requests.patch(
        request_url,
        headers={
            "authorization": f"Bearer {token}",
            "Accept": "application/json",
            "If-Match": str(version_number)
        },
        json=request_json,
        timeout=1000
    )

    print(f"PATCH {request_url}\n with data {request_json}\n got response {response.status_code}")

    if response.status_code != 204:
        print(f"WARNING: {response.status_code} response when patching address with id {asset_pk} for UPRN {asset_address.uprn} with data {asset_address}")
        add_failed_uprn(asset_address.uprn)
        return None


def load_csv_to_address_list(filename: str) -> list[AssetAddress]:
    addresses_raw = csv_to_dict_list(filename)
    addresses: list[AssetAddress] = []
    for address_raw in addresses_raw:
        addresses.append(AssetAddress.from_data(address_raw))

    return addresses

def main():
    for filename in [FAILED_PRNS_FILE, SUCCESSFUL_PRNS_FILE]:
        with open(filename, "w") as outfile:
            outfile.write("")

    addresses_to_load = load_csv_to_address_list(INPUT_FILE)


    # uprn_var = os.environ.get("test_uprn")
    asset_id = os.environ.get("test_asset_id")

    asset_table = get_dynamodb_table("Assets", STAGE)
    asset = get_asset(asset_table, asset_id)

    update_address_asset_api(ASSET_API_URL, asset.id, asset.versionNumber, asset.assetAddress)

    #TODO (proposed steps):
    # 1. Load an input source of addresses with UPRNs to update, including an "updated" column
    # 2. For each UPRN, get the address from the address API
    # 3. Get the same address from the assets Dynamo table
    # 4. Update Dynamo if the addresses are similar but not identical, Warn if they are very different
    # 5. Update the "updated" column in the input source to indicate whether the address was updated or not


if __name__ == "__main__":
    main()

    
