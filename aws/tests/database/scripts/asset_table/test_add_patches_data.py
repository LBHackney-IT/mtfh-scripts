import uuid
from moto import mock_dynamodb
from src.utils.csv_to_dict_list import csv_to_dict_list
from src.database.scripts.asset_table.add_patches_data import update_patches_for_asset
import pytest


def test_csv_to_dict_list():
    output = csv_to_dict_list("TEST_Patches.csv")

    assert isinstance(output, list)
    assert isinstance(output[0], dict)
    assert output[0]["assetId"] == "00032065"
    assert output[0]["responsibleEntities"][0]["name"] == "Fake_FirstName Fake_LastName"


@mock_dynamodb
class TestUpdatePatchesForAsset:
    def test_add_patches_data(self, asset, use_mock_dynamodb, person_patch_map):
        # --- Arrange ---
        # Create and get a fake asset table
        dynamodb = use_mock_dynamodb()
        dynamodb.create_table(
            TableName="fake-assets-table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        table = dynamodb.Table("fake-assets-table")

        # Add asset to table with known assetId
        asset["assetId"] = uuid.uuid4().hex
        table.put_item(Item=asset)

        # Modify assetId and PatchId in person_patch_map to match asset & asset patch
        person_patch_map[0]["assetId"] = asset["assetId"]
        person_patch_map[0]["PatchId"] = asset["patches"][0]["id"]

        # Modify responsible entities in person_patch_map under same assetId to different person
        new_id = uuid.uuid4().hex
        new_responsible_entities = [
            {
                "id": new_id,
                "name": "New_FirstName New_LastName",
                "personTypes": ["New_Responsible"],
            }
        ]
        person_patch_map[0]["PersonId"] = new_responsible_entities[0]["id"]
        person_patch_map[0]["PersonName"] = new_responsible_entities[0]["name"]
        person_patch_map[0]["PersonTypes"] = new_responsible_entities[0]["personTypes"]

        # --- Act ---
        with table.batch_writer() as batch_writer:
            updated_count = update_patches_for_asset(asset, person_patch_map, batch_writer)

        # --- Assert ---
        response = table.get_item(Key={"id": asset["id"]})

        # Check that the asset was updated in the database
        pytest.assume(updated_count == 1)
        pytest.assume(response["Item"]["patches"][0]["responsibleEntities"] == new_responsible_entities)

        # Check that the logs were updated
        with open("logs/logs.txt", "r") as f:
            last_log = f.readlines()[-1]
            pytest.assume(asset["assetId"] in last_log)
            pytest.assume(asset["patches"][0]["responsibleEntities"][0]["name"] in last_log)
            pytest.assume(new_responsible_entities[0]["name"] in last_log)
