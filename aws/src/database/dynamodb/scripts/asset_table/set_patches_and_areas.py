from dataclasses import asdict
from boto3.dynamodb.table import BatchWriter

from aws.src.database.domain.dynamo_domain_objects import Asset, Patch
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage


class Config:
    STAGE = Stage.HOUSING_DEVELOPMENT
    FAILED_IDS = "failed_ids.csv"  # e.g. Person-Patch-Development.csv
    LOG_FILE = "logs.txt"
    LOGGER = Logger(LOG_FILE)


def set_area_for_asset(writer: BatchWriter, asset: Asset, patches_and_areas: list[Patch]):
    """Sets the patch and area for an asset based on the asset's patch ID"""
    # Get patch and area for asset from patches and areas list
    print(f"Asset ID: {asset.id}, patches: {asset.patches}")
    hackney_area_id: str = [area.id for area in patches_and_areas if area.name.lower() == "hackney"][0]
    asset_patch_id = [patch for patch in asset.patches if patch.patchType == "patch"][0].id
    asset_patch = [patch for patch in patches_and_areas
                   if patch.id == asset_patch_id
                   and patch.patchType == "patch"][0]
    asset_area = [area for area in patches_and_areas
                  if area.id == asset_patch.parentId
                  and area.patchType == "area"][0]

    # Sanity checks
    assert asset_patch.parentId == asset_area.id, \
        f"Asset patch parent ID {asset_patch.parentId} != asset area ID {asset_area.id}"
    assert asset_area.id == hackney_area_id, \
        f"Asset area ID {asset_area.id} != Hackney area ID {hackney_area_id}"
    assert len(asset_patch.responsibleEntities) >= 1, \
        f"Asset {asset.id} patch has no responsible entities"
    assert len(asset_area.responsibleEntities) >= 1, \
        f"Asset {asset.id} area has no responsible entities"

    # Set asset patches to patch and area
    asset.patches = [asset_patch, asset_area]
    writer.put_item(asdict(asset))


def add_failed_asset_id(asset_id: str):
    with open(Config.FAILED_IDS, "a") as f:
        f.write(f"{asset_id}\n")


def main():
    asset_dynamo_table = get_dynamodb_table(table_name="Assets", stage=Config.STAGE)
    patches_dynamo_table = get_dynamodb_table(table_name="PatchesAndAreas", stage=Config.STAGE)
    patches_and_areas_raw: list[dict] = patches_dynamo_table.scan()["Items"]
    patches_and_areas: list[Patch] = [Patch.from_data(patch_or_area) for patch_or_area in patches_and_areas_raw]

    logger = Config.LOGGER
    scan = asset_dynamo_table.scan(Limit=1000)
    table_item_count = asset_dynamo_table.item_count
    progress_bar = ProgressBar(table_item_count)
    updated_count = 0
    with asset_dynamo_table.batch_writer() as writer:
        while True:
            if updated_count % 100 == 0:
                progress_bar.display(updated_count, note="Asset items processed")
            for asset_obj in scan["Items"]:
                try:
                    if not asset_obj.get("patches"):
                        raise Exception(f"Asset has no patches")
                    asset: Asset = Asset.from_data(asset_obj)
                    set_area_for_asset(writer, asset, patches_and_areas)
                    updated_count += 1
                except Exception as e:
                    logger.log(f"Error: Asset ID: {asset_obj.get('id')}, {asset_obj}, {e}")
                    add_failed_asset_id(asset_obj.get("id"))

            there_are_more_pages = "LastEvaluatedKey" in scan
            if there_are_more_pages:
                last_key = scan['LastEvaluatedKey']
                scan = asset_dynamo_table.scan(ExclusiveStartKey=last_key)
                print(f'Going to next page with ID: {last_key["id"]}')
            else:
                logger.log(f"\nFINAL TOTAL: {updated_count} updated out of {table_item_count}\n")
                break


if __name__ == "__main__":
    main()
