import json
import os
import uuid
from typing import TypedDict

import progress.bar as progress
import requests
from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.utils.csv_to_dict_list import csv_to_dict_list
from enums.enums import Stage


class AssetAddressDict(TypedDict):
    addressLine1: str | None
    addressLine2: str | None
    addressLine3: str | None
    addressLine4: str | None
    postCode: str | None
    postPreamble: str | None
    uprn: str | None


class AssetPayload(TypedDict):
    id: str
    assetId: str
    assetType: str
    assetAddress: AssetAddressDict
    assetLocation: dict
    assetCharacteristics: dict
    tenure: None
    areaId: None
    patchId: None
    rentGroup: str
    isActive: bool
    rootAsset: str
    parentAssetIds: None
    assetManagement: dict
    addDefaultSorContracts: bool


STAGE = Stage.HOUSING_PRODUCTION
FILE_PATH = "data/assets_to_load.tsv"
ASSETS_LOAD_FILE = "assets_to_load.json"


session = get_session_for_stage(STAGE)

# SSM client for fetching configuration like API URLs
ssm_client: SSMClient = session.client("ssm")

# Asset API
path_asset_api = f"/housing-tl/{STAGE.to_env_name()}/property-api-url"
asset_url = ssm_client.get_parameter(Name=path_asset_api)["Parameter"].get("Value")
assert asset_url, "Asset API URL not found in SSM"

# Hackney JWT for authenticating to APIs
hackney_jwt = os.environ.get("HACKNEY_JWT_ASSET_ADMIN")
assert hackney_jwt, "HACKNEY_JWT environment variable not set"


def create_asset_via_api(asset: dict) -> bool:
    """POST an asset to the Asset API. The API handles DynamoDB writes,
    search indexing, and event emission internally."""
    response = requests.post(
        f"{asset_url}/assets",
        json=asset,
        headers={"Authorization": f"Bearer {hackney_jwt}"},
    )
    response.raise_for_status()
    return True


def generate_assets_json() -> list[AssetPayload]:
    asset_csv_data = csv_to_dict_list(FILE_PATH, is_tsv=True)
    asset_csv_data = [row for row in asset_csv_data if row.get("Property Reference")]

    # Generate a fresh UUID for each asset (the API will handle persistence)
    asset_id_map: dict[str, str] = {
        str(item["Property Reference"]): str(uuid.uuid4()) for item in asset_csv_data
    }

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
        "uprn": "UPRN",
        "floorNo": "Floor",
        "numberOfBedrooms": "Number of Bedrooms",
    }

    def _build_asset(item: dict) -> AssetPayload:
        prop_ref = str(item["Property Reference"])
        return {
            "id": asset_id_map[prop_ref],
            "assetId": prop_ref,
            "assetType": "Dwelling",
            "assetAddress": {
                "addressLine1": item.get(column_maps["addressLine1"]) or None,
                "addressLine2": item.get(column_maps["addressLine2"]) or None,
                "addressLine3": item.get(column_maps["addressLine3"]) or None,
                "addressLine4": item.get(column_maps["addressLine4"]) or None,
                "postCode": item.get(column_maps["postCode"]) or None,
                "postPreamble": None,
                "uprn": str(item.get(column_maps["uprn"])) or None,
            },
            "assetLocation": {
                "floorNo": get_floor_number(item.get(column_maps["floorNo"])),
                "totalBlockFloors": None,
                "parentAssets": [],
            },
            "assetCharacteristics": {
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
            "tenure": None,
            "areaId": None,
            "patchId": None,
            "rentGroup": "HRA",
            "isActive": False,
            "rootAsset": "ROOT",
            "parentAssetIds": None,
            "assetManagement": {"owner": "LBH", "isCouncilProperty": True},
            "addDefaultSorContracts": True,
        }

    assets: list[AssetPayload] = [_build_asset(item) for item in asset_csv_data]

    return assets


def main():
    # 1. Generate JSON from TSV for manual inspection
    assets_dicts_o = generate_assets_json()

    with open(ASSETS_LOAD_FILE, "w") as f:
        json.dump(assets_dicts_o, f, indent=2)
    with open(ASSETS_LOAD_FILE, "r") as f:
        assets_dicts = json.load(f)

    # 2. POST each asset to the Asset API
    with progress.Bar("Uploading assets", max=len(assets_dicts)) as progress_bar:
        for asset in assets_dicts:
            create_asset_via_api(asset)
            with open("assets_loaded.txt", "a") as f:
                f.write(f"{asset['assetId']}\n")
            progress_bar.next()


if __name__ == "__main__":
    main()
