from dataclasses import asdict
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import Asset, Patch
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage
import time


class Config:
    STAGE = Stage.HOUSING_DEVELOPMENT
    FAILED_IDS = "failed_ids.csv"  # e.g. Person-Patch-Development.csv
    LOGGER = Logger("set_patches_and_areas_logs")


def set_patch_and_area_for_asset(asset_table: Table, asset: Asset, patches_and_areas: list[Patch], logger: Logger):
    """Sets the patch and area for an asset based on the asset's patch ID"""
    # Get patch and area for asset from patches and areas list
    # print(f"Asset ID: {asset.id}, patches: {[patch.name for patch in asset.patches]}")
    if len(asset.patches) == 2 and "patch" in [patch.patchType for patch in asset.patches]:
        logger.log(f"Asset {asset.id} has more than one patch - nothing to do")
        return
    hackney_area_id: str = [area.id for area in patches_and_areas if area.name.lower() == "hackney"][0]
    asset_patch = [patch for patch in asset.patches if patch.patchType == "patch"][0]
    if asset_patch.name.lower() == "e2e":
        logger.log(f"Asset {asset.id} is an E2E patch")
        return
    try:
        asset_patch = [patch for patch in patches_and_areas
                       if patch.id == asset_patch.id
                       and patch.patchType == "patch"][0]
        asset_area = [area for area in patches_and_areas
                      if area.id == asset_patch.parentId
                      and area.patchType == "area"][0]
    except IndexError:
        logger.log(f"Index error for asset {asset.id}")
        return

    # Sanity checks
    assert asset_patch.parentId == asset_area.id, \
        f"Asset patch parent ID {asset_patch.parentId} != asset area ID {asset_area.id}"
    assert asset_area.parentId == hackney_area_id, \
        f"Asset area ID {asset_area.id} != Hackney area ID {hackney_area_id}"
    assert len(asset_patch.responsibleEntities) >= 1, \
        f"Asset {asset.id} patch has no responsible entities"
    assert len(asset_area.responsibleEntities) >= 1, \
        f"Asset {asset.id} area has no responsible entities"

    # Set asset patches to patch and area
    asset.patches = [asset_patch, asset_area]
    if not asset.versionNumber:
        asset.versionNumber = 0
    else:
        asset.versionNumber += 1
    asset_dict = asdict(asset)
    for patch in asset_dict["patches"]:
        patch.pop("versionNumber", None)
    try:
        logger.log(f"Asset ID: {asset.id}, New patches: {[patch.name for patch in asset.patches]}")
        return
        asset_table.update_item(
            Key={"id": asset.patch_id},
            UpdateExpression=f"SET patches = :r, versionNumber = :v",
            ExpressionAttributeValues={":r": asset_dict["patches"], ":v": asset.versionNumber},
            ReturnValues="NONE",
        )
    except Exception as e:
        raise e


def add_failed_asset_id(asset_id: str):
    with open(Config.FAILED_IDS, "a") as f:
        f.write(f"{asset_id}\n")


def set_patches_and_areas(start_pk: str | None = None):
    logger = Config.LOGGER
    asset_dynamo_table = get_dynamodb_table(table_name="Assets", stage=Config.STAGE)
    patches_dynamo_table = get_dynamodb_table(table_name="PatchesAndAreas", stage=Config.STAGE)
    patches_and_areas_raw: list[dict] = patches_dynamo_table.scan()["Items"]
    patches_and_areas: list[Patch] = []
    for patch_raw in patches_and_areas_raw:
        try:
            patches_and_areas.append(Patch.from_data(patch_raw))
        except Exception as e:
            logger.log(f"Error converting Patch ID: {patch_raw.get('id')} to type Patch")
            print(e)

    start_key = {"id": start_pk} if start_pk else None
    scan = asset_dynamo_table.scan(Limit=1000, ExclusiveStartKey=start_key)
    table_item_count = asset_dynamo_table.item_count
    progress_bar = ProgressBar(table_item_count)
    processed_count = 39363
    while True:
        cycle_start_time = time.time()
        for asset_obj in scan["Items"]:
            try:
                if not asset_obj.get("patches"):
                    logger.log(f"Asset ID: {asset_obj.get('id')} has no patches")
                    continue
                asset: Asset = Asset.from_data(asset_obj)
                set_patch_and_area_for_asset(asset_dynamo_table, asset, patches_and_areas, logger)
                pass
            except Exception as e:
                logger.log(f"Error: Asset ID: {asset_obj.get('id')}, {e}")
                add_failed_asset_id(f'{asset_obj.get("id")} - {e}')
                # raise e

        there_are_more_pages = "LastEvaluatedKey" in scan
        if there_are_more_pages:
            processed_count += len(scan["Items"])
            print(f"{len(scan['Items'])} processed in {round(time.time() - cycle_start_time):,d}s")
            print(f'Going to next page with ID: {scan["LastEvaluatedKey"]["id"]} - processed {processed_count} items so far')
            progress_bar.display(processed_count, note="Asset items processed")
            scan = asset_dynamo_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        else:
            logger.log(f"\nFINAL TOTAL: {processed_count} updated out of {table_item_count}\n")
            break


if __name__ == "__main__":
    start_time = time.time()
    set_patches_and_areas(start_pk=None)
    print(f"Time taken: {round(time.time() - start_time):,d}s")

