"""
Reads a CSV file (headers: "uprn", "proposed_housing_management_area") and updates the
assetAddress.neighbourhood field on matching Assets DynamoDB records, looked up by UPRN.

Strategy:
  1. Load the CSV into a {uprn: neighbourhood} lookup dict (value uppercased).
  2. Scan the entire Assets table once, building a {uprn: item_id} index.
  3. Compare both sets:
     - Match  → update_item to set assetAddress.neighbourhood
     - Table only → tracked as "Skipped: Table Only"
     - CSV only   → tracked as "Skipped: CSV Only"
  4. Write a structured migration_log.txt summary alongside the standard Logger output.
"""

from dataclasses import dataclass
from pathlib import Path

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage
from utils.confirm import confirm


@dataclass
class Config:
    TABLE_NAME = "Assets"
    STAGE = Stage.HOUSING_DEVELOPMENT
    LOGGER = Logger("patch_asset_neighbourhood_from_uprn")
    FILE_PATH = str(Path(__file__).parent / "input" / "uprn_neighbourhood.csv")
    LOG_FILE = str(Path(__file__).parent / "migration_log.txt")


def load_csv_lookup(file_path: str) -> dict[str, str]:
    """
    Load CSV rows into a {uprn: neighbourhood} dict.
    neighbourhood values are uppercased. csv_to_dict_list may JSON-decode numeric UPRNs
    to int, so all keys are normalised to str.
    """
    rows = csv_to_dict_list(file_path)
    lookup: dict[str, str] = {}
    for row in rows:
        uprn = str(row["uprn"]).strip()
        neighbourhood = str(row["proposed_housing_management_area"]).strip().upper()
        if uprn:
            lookup[uprn] = neighbourhood
    Config.LOGGER.log(f"Loaded {len(lookup)} UPRN entries from CSV: {file_path}")
    return lookup


def scan_table_uprn_index(table: Table) -> dict[str, str]:
    """
    Paginated scan of the Assets table.
    Returns {uprn: item_id} for every record that carries a non-empty assetAddress.uprn.
    ProjectionExpression limits network payload to the two fields we actually need.
    """
    Config.LOGGER.log("Scanning Assets table to build UPRN index (this may take a while)...")
    uprn_index: dict[str, str] = {}

    response = table.scan(
        ProjectionExpression="#item_id, assetAddress",
        ExpressionAttributeNames={"#item_id": "id"},
    )
    for item in response.get("Items", []):
        asset_address = item.get("assetAddress") or {}
        uprn = asset_address.get("uprn")
        if uprn:
            uprn_index[str(uprn).strip()] = item["id"]

    while "LastEvaluatedKey" in response:
        Config.LOGGER.log(f"  Scanned {len(uprn_index)} UPRNs so far — paginating...")
        response = table.scan(
            ProjectionExpression="#item_id, assetAddress",
            ExpressionAttributeNames={"#item_id": "id"},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response.get("Items", []):
            asset_address = item.get("assetAddress") or {}
            uprn = asset_address.get("uprn")
            if uprn:
                uprn_index[str(uprn).strip()] = item["id"]

    Config.LOGGER.log(f"Indexed {len(uprn_index)} UPRN entries from the Assets table")
    return uprn_index


def patch_neighbourhood(
    table: Table,
    csv_lookup: dict[str, str],
    table_uprn_index: dict[str, str],
) -> tuple[list[str], list[str], list[str], list[tuple[str, str]]]:
    """
    Compares the two UPRN sets, runs update_item for each match,
    and returns (updated, skipped_table_only, skipped_csv_only, errors).
    """
    csv_uprns = set(csv_lookup.keys())
    table_uprns = set(table_uprn_index.keys())

    matching_uprns = csv_uprns & table_uprns
    skipped_csv_only = sorted(csv_uprns - table_uprns)
    skipped_table_only = sorted(table_uprns - csv_uprns)

    Config.LOGGER.log(f"Matching UPRNs:        {len(matching_uprns)}")
    Config.LOGGER.log(f"Skipped (CSV Only):    {len(skipped_csv_only)}")
    Config.LOGGER.log(f"Skipped (Table Only):  {len(skipped_table_only)}")

    updated: list[str] = []
    errors: list[tuple[str, str]] = []
    matching_list = sorted(matching_uprns)
    progress_bar = ProgressBar(len(matching_list))

    for i, uprn in enumerate(matching_list):
        if i % 100 == 0:
            progress_bar.display(i)

        item_id = table_uprn_index[uprn]
        neighbourhood = csv_lookup[uprn]

        try:
            table.update_item(
                Key={"id": item_id},
                UpdateExpression="SET assetAddress.neighbourhood = :n",
                ExpressionAttributeValues={":n": neighbourhood},
                ReturnValues="NONE",
            )
            updated.append(uprn)
        except Exception as e:
            Config.LOGGER.log(f"Failed to update UPRN {uprn} (id: {item_id}): {e}")
            errors.append((uprn, str(e)))

    return updated, skipped_table_only, skipped_csv_only, errors


def write_migration_log(
    log_file: str,
    updated: list[str],
    skipped_table_only: list[str],
    skipped_csv_only: list[str],
    errors: list[tuple[str, str]],
) -> None:
    with open(log_file, "w") as f:
        f.write("=== Migration Log: patch_asset_neighbourhood_from_uprn ===\n\n")

        f.write(f"--- Skipped: Table Only ({len(skipped_table_only)} records) ---\n")
        f.write("    (UPRNs present in DynamoDB but absent from the CSV)\n")
        for uprn in skipped_table_only:
            f.write(f"    {uprn}\n")
        f.write("\n")

        f.write(f"--- Skipped: CSV Only ({len(skipped_csv_only)} records) ---\n")
        f.write("    (UPRNs present in the CSV but not found in DynamoDB)\n")
        for uprn in skipped_csv_only:
            f.write(f"    {uprn}\n")
        f.write("\n")

        if errors:
            f.write(f"--- Errors ({len(errors)} records) ---\n")
            for uprn, err in errors:
                f.write(f"    {uprn}: {err}\n")
            f.write("\n")

        total = len(updated) + len(skipped_table_only) + len(skipped_csv_only) + len(errors)
        f.write("=== Summary ===\n")
        f.write(f"    Updated:               {len(updated)}\n")
        f.write(f"    Skipped (Table Only):  {len(skipped_table_only)}\n")
        f.write(f"    Skipped (CSV Only):    {len(skipped_csv_only)}\n")
        f.write(f"    Errors:                {len(errors)}\n")
        f.write(f"    Total processed:       {total}\n")

    Config.LOGGER.log(f"Migration log written to: {log_file}")


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)

    csv_lookup = load_csv_lookup(Config.FILE_PATH)
    table_uprn_index = scan_table_uprn_index(table)

    matching_count = len(set(csv_lookup.keys()) & set(table_uprn_index.keys()))

    if not confirm(
        f"This will update assetAddress.neighbourhood for {matching_count} matching assets "
        f"in '{Config.TABLE_NAME}' on {Config.STAGE.to_env_name()}. Proceed?"
    ):
        Config.LOGGER.log("Aborted by user.")
        return

    updated, skipped_table_only, skipped_csv_only, errors = patch_neighbourhood(
        table, csv_lookup, table_uprn_index
    )

    write_migration_log(Config.LOG_FILE, updated, skipped_table_only, skipped_csv_only, errors)

    Config.LOGGER.log(f"Updated:               {len(updated)}")
    Config.LOGGER.log(f"Skipped (Table Only):  {len(skipped_table_only)}")
    Config.LOGGER.log(f"Skipped (CSV Only):    {len(skipped_csv_only)}")
    Config.LOGGER.log(f"Errors:                {len(errors)}")
    Config.LOGGER.log_end()


if __name__ == "__main__":
    main()
