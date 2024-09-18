from decimal import Decimal
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.database.opensearch.client.elasticsearch_client import LocalElasticsearchClient
from enums.enums import Stage
from datetime import datetime
import json


STAGE = Stage.HOUSING_PRODUCTION


def create_asset_search_from_dynamodb(asset_id: str) -> dict:
    assets_table = get_dynamodb_table("Assets", STAGE)
    asset_dynamodb = assets_table.get_item(Key={"id": asset_id})
    assert "Item" in asset_dynamodb, f"Asset with id {asset_id} not found in DynamoDB"
    asset = asset_dynamodb["Item"]

    def is_tenure_active(asset: dict) -> bool:
        if not asset.get("tenure"):
            return False
        tenure = asset["tenure"]
        if tenure.get("isActive") is False:
            return False
        if tenure.get("endOfTenureDate"):
            parsed_eot = datetime.strptime(
                tenure["endOfTenureDate"], "%Y-%m-%dT%H:%M:%SZ"
            )
            if parsed_eot < datetime.now():
                return False

        return True

    def resolve_asset_location(asset: dict) -> dict:
        if asset.get("assetLocation"):
            return {"floorNo": asset["assetLocation"].get("floorNo")}
        return {}

    def resolve_asset_characteristics(asset: dict) -> dict:
        if not asset.get("assetCharacteristics"):
            return {}
        characteristics = asset["assetCharacteristics"]
        for key, value in characteristics.items():
            if isinstance(value, Decimal):
                characteristics[key] = int(value)
            if "numberOf" in key:
                if value is None:
                    characteristics[key] = 0
        return characteristics

    asset_search_object = {
        "id": asset["id"],
        "assetId": asset["assetId"],
        "assetType": asset["assetType"],
        "isAssetCautionaryAlerted": False,
        "assetAddress": asset.get("assetAddress", {}),
        "tenure": asset.get("tenure", None),
        "rootAsset": asset["rootAsset"],
        "isActive": is_tenure_active(asset),
        "parentAssetIds": asset["parentAssetIds"],
        "assetCharacteristics": resolve_asset_characteristics(asset),
        "assetManagement": asset["assetManagement"],
        "assetStatus": asset.get("assetStatus"),
        "assetLocation": resolve_asset_location(asset),
        "assetContract": asset.get("assetContract"),
    }

    assert "id" in asset_search_object, "Asset id is missing in the search object"
    return asset_search_object


def index_es_asset(asset_search_object: dict) -> None:
    es_client = LocalElasticsearchClient("assets", 9200)
    current = es_client.es_instance.get(index="assets", id=asset_search_object["id"])
    if current:
        with open("BACKUP_ASSET.json", "w") as f:
            json.dump(current, f, indent=2)
        print(f"Index already exists: {current}")
        assert input("Continue? (y/n): ").lower() == "y"
    es_client.index(doc_id=asset_search_object["id"], body=asset_search_object)
    print(f"Indexed asset {asset_search_object['id']} in Elasticsearch")


if __name__ == "__main__":
    ASSET_ID = "e00ba652-d0ed-5a40-31f5-a2a02f554c7a"
    new_asset_search = create_asset_search_from_dynamodb(ASSET_ID)
    index_es_asset(new_asset_search)
