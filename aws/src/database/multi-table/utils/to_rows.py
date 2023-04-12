from datetime import date

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.utils.logger import Logger

logger = Logger(log_file_name=f"to_rows_{date.today()}.log")


class DataSource:
    person = "person"
    tenure = "tenure"
    asset = "asset"
    property_alert_new = "PropertyAlertNew"


def find_matches_in_table(table: Table, index_name: str, key: str, value: str) -> dict:
    batch_keys = {
        table.name: {
            "Keys": [
                {key: value}
            ]
        }
    }
    response = table.meta.client.batch_get_item(RequestItems=batch_keys)


def cautionary_alert_to_row(cautionary_alert: dict, key_list: list[str], index: int) -> dict:
    ca_row = {key: "" for key in key_list}
    ca_row["match_id"] = index
    ca_row["source"] = DataSource.property_alert_new
    ca_row["address_(merged)"] = f"{cautionary_alert['door_number']} {cautionary_alert['address']}"
    ca_row["property_reference"] = f"{cautionary_alert['property_reference']}"
    ca_row["uprn"] = f"{cautionary_alert['uprn']}"
    ca_row["person_name"] = f"{cautionary_alert['person_name']}"
    ca_row["mmh_id"] = f"{cautionary_alert['mmh_id']}"
    ca_row["outcome"] = f"{cautionary_alert['outcome']}"
    ca_row["assure_reference"] = f"{cautionary_alert['assure_reference']}"
    ca_row["caution_on_system"] = f"{cautionary_alert['caution_on_system']}"
    ca_row["code"] = f"{cautionary_alert['code']}"
    logger.log(f"ca_row: {ca_row}")
    return ca_row


def asset_to_row(asset: dict, header_row: list[str], index: int) -> dict:
    asset_row = {key: "" for key in header_row}
    asset_row["match_id"] = index
    asset_row["source"] = DataSource.asset
    asset_row["address_(merged)"] = asset["assetAddress"]["addressLine1"]
    asset_row["property_reference"] = asset["assetId"]
    asset_row["uprn"] = asset["assetAddress"]["uprn"]
    # NOTE: No person details
    logger.log(f"asset_row: {asset_row}")
    return asset_row  #


def tenure_to_row(ca_mmh_id: str, tenure: dict, header_row: list[str], index: int) -> dict:
    tenure_row = {key: "" for key in header_row}
    tenure_row["match_id"] = index
    tenure_row["source"] = DataSource.tenure
    tenure_row["address_(merged)"] = tenure["tenuredAsset"]["fullAddress"]
    tenure_row["property_reference"] = tenure["tenuredAsset"]["propertyReference"]
    tenure_row["uprn"] = tenure["tenuredAsset"]["uprn"]

    household_members = tenure["householdMembers"]
    for household_member in household_members:
        member_person_id = household_member["id"]
        if member_person_id == ca_mmh_id:
            tenure_row["person_name"] = f"{household_member['person_name']}"
            tenure_row["mmh_id"] = f"{household_member['mmh_id']}"
            break
    else:
        logger.log(f"\nDidn't find person {ca_mmh_id} in householdMembers {household_members}\n")
        tenure_row["person_name"] = None
        tenure_row["mmh_id"] = None
    logger.log(f"tenure_row: {tenure_row}")
    return tenure_row


def person_to_row(matching_person: dict | None, header_row: list[str], i: int) -> dict:
    person_row = {key: "" for key in header_row}
    person_row["match_id"] = i
    person_row["source"] = DataSource.person

    if matching_person is None:
        for key in header_row:
            if key not in ["match_id", "mismatch"]:
                person_row[key] = "PersonNotFound"
        return person_row
    """
    public static bool IsTenureActive(DateTime? endDate)
    {
        return !endDate.HasValue
            || endDate.Value.Date == _defaultEndDate.Date
            || DateTime.UtcNow <= endDate.Value;
    }
    """

    def _is_tenure_active(tenure: dict) -> bool:
        if tenure["endDate"] is None:
            return True
        end_date = tenure["endDate"]
        year, month, day = [int(part) for part in end_date.split("T")[0].split("-")]

        end_date_parsed = date(year, month, day)
        today = date.today()

        tenure_active = end_date_parsed >= today or "1900" in end_date
        return tenure_active

    # Find first (and only) active tenure
    for tenure in matching_person["tenures"]:
        if _is_tenure_active(tenure):
            person_row["address_(merged)"] = tenure["assetFullAddress"]
            person_row["property_reference"] = f"{tenure['propertyReference']}"
            person_row["uprn"] = f"{tenure['uprn']}"
            break
    else:
        person_row["address_(merged)"] = ""

    person_row["person_name"] = f"{matching_person['title'] or matching_person['preferredTitle']} " \
                                f"{matching_person['firstName']} {matching_person['surname']}"
    person_row["mmh_id"] = f"{matching_person['id']}"
    logger.log(f"person_row: {person_row}")
    return person_row
