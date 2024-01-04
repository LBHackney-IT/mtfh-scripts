import csv
import re

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage

STAGE = Stage.HOUSING_PRODUCTION

logger = Logger()


def set_assure_ref_for_alert(alert_item: dict):
    assure_ref_regex = re.compile(r"[0-9]{3,}")
    if alert_item["Action on Assure"]:
        alert_item["Action on Assure"] = str(alert_item["Action on Assure"])
        if "2020" not in alert_item["Action on Assure"]:
            res = re.search(assure_ref_regex, alert_item["Action on Assure"])
            if res is not None:
                assure_ref = res.group(0)
                alert_item["assure_ref"] = assure_ref
                return
    alert_item["assure_ref"] = None


def clean_asset_id(asset_id: str) -> str | None:
    asset_id = str(asset_id)
    asset_id = asset_id.replace(" ", "")
    if len(asset_id) < 8:
        # pad with zeros
        asset_id = asset_id.zfill(8)
    if asset_id.isnumeric():
        if len(asset_id) > 8:
            return None
        propref_regex = re.compile(r"^([0-9]{8})$")
        asset_id = propref_regex.findall(asset_id)[0]
        return asset_id
    return None


def resolve_correct_person_from_household_members(alert_item, household_members) -> str | None:
    name_from_alert = alert_item["Name"]
    remove_non_alpha = r"\W+"
    name_from_alert = re.sub(remove_non_alpha, " ", name_from_alert)
    for member in household_members:
        member_name = re.sub(remove_non_alpha, " ", member["fullName"])
        if member_name.strip().lower() in name_from_alert.strip().lower():
            return member["id"]
    else:
        return None


def get_person_from_tenure(alert_item: dict, tenure: dict) -> str | None:
    household_members = tenure["householdMembers"]
    if len(household_members) == 0:
        alert_item["failed_reason"] = f"No HouseholdMembers found for tenure {tenure['id']}"
        return None
    elif len(household_members) == 1:
        person_id = household_members[0].get("id")
        if person_id is None:
            alert_item["failed_reason"] = f"Couldn't get PersonID from HouseholdMembers for tenure {tenure['id']}"
        person_id = resolve_correct_person_from_household_members(alert_item, household_members)
        if person_id is None:
            alert_item["failed_reason"] = f"PERSON IS NOT TENURE MEMBER {alert_item['Name']} " \
                                          f"in tenure {tenure['id']}: " \
                                          f"{(household_members[0]['fullName'], household_members[0]['id'])}"
        else:
            return person_id
    else:
        person_id = resolve_correct_person_from_household_members(alert_item, household_members)
        if person_id is None:
            alert_item["failed_reason"] = f"PERSON IS NOT IN TENURE MEMBERS {alert_item['Name']} " \
                                          f"in tenure {tenure['id']}: " \
                                          f"{[(household_member['fullName'], household_member['id']) for household_member in household_members]}"
            alert_item["failed_reason"] = alert_item["failed_reason"].replace("'", "")
        return person_id


def set_person_ids_in_alert_data(asset_table: Table, tenure_table: Table, alerts_from_csv: list[dict]):
    """
    Iterate through the alerts and set the personId for each alert
    :return: A dictionary containing the updated alerts and the alerts that failed to update
    """
    progress_bar = ProgressBar(len(alerts_from_csv))
    for i, alert_item in enumerate(alerts_from_csv):
        if i % 10 == 0:
            progress_bar.display(i)
        asset_id = alert_item["Property Reference"]
        asset_id = clean_asset_id(asset_id)
        if asset_id is None:
            alert_item["failed_reason"] = f"Invalid assetId: {alert_item['Property Reference']}. " \
                                          f"Alert name: {alert_item['Name']}"
            continue
        results = get_by_secondary_index(asset_table, "AssetId", "assetId", asset_id)
        if len(results) > 1:
            alert_item["failed_reason"] = f"Multiple assets found for assetId {alert_item['Property Reference']}. " \
                                          f"Alert name: {alert_item['Name']}"
            continue
        elif len(results) == 0:
            alert_item["failed_reason"] = f"No assets found for assetId {alert_item['Property Reference']}. " \
                                          f"Alert name: {alert_item['Name']}"
            continue
        asset = results[0]

        tenure_id = asset.get("tenure").get("id")
        if tenure_id is None:
            alert_item["failed_reason"] = f"No tenure ID for asset {asset['assetId']}"
            continue
        tenure = tenure_table.get_item(Key={"id": tenure_id}).get("Item")
        if tenure is None:
            alert_item["failed_reason"] = f"Could not get tenure for assetId {asset['assetId']}"
            continue

        person_id = get_person_from_tenure(alert_item, tenure)

        alert_item["mmh_id"] = person_id


def main():
    _file_path = "../data/input/spreadsheet_alerts_v2.csv"
    alert_data = csv_to_dict_list(_file_path)

    for alert in alert_data:
        alert["failed_reason"] = None
    for alert in alert_data:
        if alert["OUTCOME"] in [None, ""] and alert["Lookup"] in [None, ""]:
            alert["failed_reason"] = "No Outcome and no Lookup"

    alerts_to_be_processed = [alert for alert in alert_data if alert["failed_reason"] is None]
    for alert in alerts_to_be_processed:
        set_assure_ref_for_alert(alert)

    tenure_table: Table = get_dynamodb_table("TenureInformation", STAGE)
    asset_table: Table = get_dynamodb_table("Assets", STAGE)
    set_person_ids_in_alert_data(asset_table, tenure_table, alert_data)

    with open("../data/output/fixed_alerts.csv", "w") as output_file:
        keys = alert_data[0].keys()
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(alert_data)


if __name__ == "__main__":
    main()
