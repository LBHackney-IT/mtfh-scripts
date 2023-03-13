from pathlib import Path

from boto3.dynamodb.table import BatchWriter
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.enums.enums import Stage
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar


class Config:
    STAGE = Stage.DEVELOPMENT
    WORKDIR = Path(__file__).parent  # gets current directory *even when run as module*
    CSV_FILE = f"{WORKDIR}/input/Person-Patch-{str(STAGE).title()}.csv"  # e.g. Person-Patch-Development.csv
    LOG_FILE = f"{WORKDIR}/logs/logs.txt"
    LOGGER = Logger(LOG_FILE)


def update_patches_for_asset(asset: dict, person_patch_map: list[dict], dynamo_table_writer: BatchWriter,
                             logger: Logger = None) -> int:
    """
    :param asset: Asset to update from the database
    :param person_patch_map: List of dicts with keys "PatchId", "PersonId", "PersonName", "PersonTypes"
    :param dynamo_table_writer: DynamoDB batch writer object
    :param logger: Logger object
    :return: Number of updates made
    """
    update_count = 0
    if "patches" not in asset.keys():
        return 0
    new_responsible_entities = []
    old_responsible_entities = []
    for person_patch in person_patch_map:
        for i, asset_patch in enumerate(asset["patches"]):
            if person_patch["PatchId"] == asset_patch["id"]:
                new_responsible_entity = {
                    "id": person_patch["PersonId"],
                    "name": person_patch["PersonName"],
                    "personTypes": person_patch["PersonTypes"]
                }
                if new_responsible_entity not in new_responsible_entities:
                    new_responsible_entities.append(new_responsible_entity)
            old_responsible_entities += asset_patch["responsibleEntities"]
            asset["patches"][i]["responsibleEntities"] = new_responsible_entities
    if new_responsible_entities != old_responsible_entities:
        logger and logger.log(f"{asset['assetId']} || "
                              f"{[person['name'] for person in old_responsible_entities]} ->> "
                              f"{[person['name'] for person in new_responsible_entities]}")
        dynamo_table_writer.put_item(asset)
        update_count = len([person for person in new_responsible_entities if person not in old_responsible_entities])
    return update_count


def main(asset_dynamo_table: Table):
    """
    Updates patch data for all assets
    :param asset_dynamo_table: DynamoDB table object
    :return:
    """
    logger = Config.LOGGER
    scan = asset_dynamo_table.scan(Limit=1000)
    table_item_count = asset_dynamo_table.item_count
    progress_bar = ProgressBar(table_item_count)
    person_patch_map: list[dict] = csv_to_dict_list(Config.CSV_FILE)
    with asset_dynamo_table.batch_writer() as writer:
        total_count = 0
        updated_count = 0
        while True:
            progress_bar.display(total_count, note="Asset items processed")
            if continue_confirm.lower() not in ["yes", "y"]:
                print("Aborting")
                break
            for asset in scan["Items"]:
                total_count += 1
                updated_count += update_patches_for_asset(asset, person_patch_map, writer, logger)

            if total_count % 100 == 0:
                progress_bar.display(total_count, note="Asset items processed")
                progress_bar.display(updated_count, note="Asset items updated")

            there_are_more_pages = "LastEvaluatedKey" in scan
            if there_are_more_pages:
                last_key = scan['LastEvaluatedKey']
                scan = asset_dynamo_table.scan(ExclusiveStartKey=last_key)
                print(f'Going to next page with ID: {last_key["id"]}, AssetID: {asset["assetId"]})')
            else:
                logger.log(f"\nFINAL TOTAL: {total_count} items processed, {updated_count} updated\n")
                break


if __name__ == "__main__":
    continue_confirm = input(f"Running in {Config.STAGE}, continue? y/n: ")
    Config.LOGGER.log_file_name = f"{Config.STAGE}_add_patches_data.log"
    table = get_dynamodb_table(table_name="Assets", stage=Config.STAGE)
    main(table)
