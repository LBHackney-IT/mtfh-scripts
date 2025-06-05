from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage
from utils.confirm import confirm


@dataclass
class Config:
    TABLE_NAME = "TenureInformation"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_STAGING

def add_tenure_to_tag_ref(tenure_table: Table, tenure_from_csv: list[dict], logger: Logger) -> int:
    """
    update the tenure record to have tag ref data given from sql db

    """
    update_count = 0
    progress_bar = ProgressBar(len(tenure_from_csv))
    for i, csv_asset_item in enumerate(tenure_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)
        tenure_pk = csv_asset_item["id"].strip()
        tag_ref = csv_asset_item["tag_ref"].strip()

        dynamo_tenure = tenure_table.get_item(Key={"id": tenure_pk})
        dynamo_tenure = dynamo_tenure.get("Item")
        legacy_ref = dynamo_tenure.get("legacyReferences") if dynamo_tenure else None

        if dynamo_tenure is None:
            print(f"Tenure with id {tenure_pk} not found in the table.")
            continue

        if legacy_ref in [None, []]:
            print(f"No legacyReference assigned for tenure: {dynamo_tenure.get('id')} - creating object")
            legacy_ref = [{"name": "uh_tag_ref", "value": tag_ref if tag_ref else None}]
            dynamo_tenure["legacyReferences"] = legacy_ref

        if isinstance(legacy_ref, list):
            for legacy in legacy_ref:
                if legacy.get("name") == "uh_tag_ref":
                    legacy["value"] = tag_ref if tag_ref else None
                else:
                    new_legacy_ref = {"name": "uh_tag_ref", "value": tag_ref if tag_ref else None}
                    legacy_ref.append(new_legacy_ref)

        tenure_table.put_item(Item=dynamo_tenure)
        update_count += 1
        logger.log(f"Updated {update_count} records")
    return update_count


def add_missing_tenure_refs():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = r"linked_tenures_staging.csv"
    tag_ref_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER
    if confirm(f"Are you sure you want to update property patch in {Config.STAGE.to_env_name()}?"):
        add_tenure_to_tag_ref(table, tag_ref_csv_data, Config.LOGGER)
        logger.log(f"Updated {len(tag_ref_csv_data)} tenures with tag ref data in {Config.STAGE.to_env_name()}")

if __name__ == "__main__":
    add_missing_tenure_refs()