import csv
from datetime import date

from mypy_boto3_dynamodb import ServiceResource
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.authentication.generate_aws_resource import generate_aws_service
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.is_uuid_valid import is_uuid_valid
from aws.src.utils.logger import Logger
from enums.enums import Stage
from utils.to_rows import cautionary_alert_to_row, person_to_row

STAGE = Stage.HOUSING_PRODUCTION
OUTFILE = "../data/output.csv"

logger = Logger()


def persons_from_ca(cautionary_alerts: list[dict], person_table: Table, dynamo_resource: ServiceResource) -> list[dict]:
    persons = []

    mmh_ids = list(set([alert["mmh_id"] for alert in cautionary_alerts if is_uuid_valid(alert["mmh_id"])]))

    # mmh_ids = mmh_ids[0:100]

    person_ids = [{"id": mmh_id} for mmh_id in mmh_ids]
    for i in range(0, len(person_ids), 100):
        batch_keys = person_ids[i:i + 100]
        response = dynamo_resource.batch_get_item(RequestItems={
            person_table.name: {
                "Keys": batch_keys
            },
        })
        persons.extend([person for person in response["Responses"][person_table.name]])
    for i, person in enumerate(persons):
        person["matchId"] = i
    return persons


def tenures_from_persons(persons: list[dict], tenure_table: Table, dynamo_resource: ServiceResource) -> list[dict]:
    tenures = []

    def is_tenure_active(tenure: dict) -> bool:
        if tenure["endDate"] is None:
            return True
        end_date = tenure["endDate"]
        year, month, day = [int(part) for part in end_date.split("T")[0].split("-")]

        end_date_parsed = date(year, month, day)
        today = date.today()

        tenure_active = end_date_parsed >= today or "1900" in end_date
        return tenure_active

    def get_active_tenure_id(person: dict) -> str | None:
        tenures = person["tenures"]
        active_tenure_ids = [tenure["id"] for tenure in tenures if is_tenure_active(tenure)]
        if len(active_tenure_ids) > 1:
            logger.log(f"WARNING: More than one active tenure for person {person['id']}")
        if len(active_tenure_ids) == 0:
            logger.log(f"WARNING: No active tenure for person {person['id']}")
            return None
        return active_tenure_ids[0]

    tenure_ids = [get_active_tenure_id(person) for person in persons if get_active_tenure_id(person) is not None]

    tenure_keys = [{"id": tenure_ids} for tenure_ids in tenure_ids]
    for i in range(0, len(tenure_keys), 100):
        response = dynamo_resource.batch_get_item(RequestItems={
            tenure_table.name: {
                "Keys": tenure_keys[i:i + 100]
            },
        })
        tenures.extend([tenure for tenure in response["Responses"][tenure_table.name]])
    for i, tenure in enumerate(tenures):
        tenure["matchId"] = i
    for tenure in tenures:
        for person in persons:
            if tenure["id"] == get_active_tenure_id(person):
                tenure["matchId"] = person["matchId"]
    return tenures


def main():
    header_row = [
        "match_id", "source",
        "address_(merged)", "property_reference", "uprn",  # Property
        "person_name", "mmh_id",  # Person
        "outcome", "assure_reference", "caution_on_system", "code",  # Cautionary Alert
        "mismatch",  # Mismatch boolean
        "mismatch_details"  # List of mismatched fields
    ]

    _file_path = "../data/cautionary_alerts.csv"
    alert_csv_data = csv_to_dict_list(_file_path)

    dynamo_resource: ServiceResource = generate_aws_service("dynamodb", STAGE)
    person_table: Table = get_dynamodb_table("Persons", STAGE, dynamo_resource)
    tenure_table: Table = get_dynamodb_table("TenureInformation", STAGE, dynamo_resource)
    asset_table: Table = get_dynamodb_table("Assets", STAGE, dynamo_resource)

    all_rows = [
        header_row
    ]

    # Dataset 1 - mmhId -> Person -> Tenure -> Asset
    persons = persons_from_ca(alert_csv_data, person_table, dynamo_resource)
    for person in persons:
        for alert in alert_csv_data:
            if alert["mmh_id"] == person["id"]:
                alert["matchId"] = person["matchId"]

    filtered_alerts = [alert for alert in alert_csv_data if "matchId" in alert.keys()]

    tenures = tenures_from_persons(persons, tenure_table, dynamo_resource)

    ca_row_data: list[dict] = [cautionary_alert_to_row(alert, header_row, alert["matchId"]) for alert in
                               filtered_alerts]
    ca_csv_rows: list[list[str]] = [[value for value in datum.values()] for datum in ca_row_data]

    person_row_data: list[dict] = [person_to_row(person, header_row, person["matchId"]) for person in persons]
    person_csv_rows: list[list[str]] = [[value for value in datum.values()] for datum in person_row_data]
    all_rows.extend(ca_csv_rows)
    all_rows.extend(person_csv_rows)

    with open(OUTFILE, "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(all_rows)


if __name__ == "__main__":
    main()
