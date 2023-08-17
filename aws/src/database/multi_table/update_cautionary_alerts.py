
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from enums.enums import Stage
from dateutil import parser
from datetime import datetime

STAGE = Stage.HOUSING_PRODUCTION

def update_cautionary_alert_from_csv_if_tenure_with_alerts_inactive(person_table: Table, alerts_from_csv: list[dict]) -> list[dict]:
    """
    Iterate through the alerts and remove property details if tenure is inactive
    1. Get person details from dynamo using mmh_id in csv
    2. check if there is an end date within the tenure object
    3. if so remove the property details from the csv
    """

    for alert_item in alerts_from_csv:
        alert_id = alert_item["id"]
        mmh_id = alert_item["mmh_id"]
        if mmh_id is '' or None:
            alert_item["failed_reason"] = f"mmh_id is null for id {alert_id}"
            continue
        person = person_table.get_item(Key={"id": mmh_id}).get("Item")

        person_id = person["id"]
        if person is None:
            alert_item["failed_reason"] = f"Could not get person for personId {person_id}"
            continue
        tenures = person["tenures"]
        for tenure_in_person in tenures:
            end_date = tenure_in_person["endDate"]
            if end_date is None:
                continue
            parsed_end_date: datetime = parser.parse(timestr=end_date.split("T")[0])
            if parsed_end_date < datetime.now(): 
                prop_ref = alert_item["property_reference"]
                if str(tenure_in_person["propertyReference"]) == str(prop_ref):
                    alert_item["address"] = None
                    alert_item["property_reference"] = None
                    alert_item["uprn"] = None    
                    alert_item["door_number"] = None          
    return alerts_from_csv
     
def main():
    _file_path = "aws/src/database/data/input/CautionaryAlerts14August2023.csv"
    alert_data = csv_to_dict_list(_file_path)
    person_table: Table = get_dynamodb_table("Persons", STAGE)
    results = update_cautionary_alert_in_db_if_tenure_inactive(person_table, alert_data)

    with open("updated_alerts.tsv", "w") as f:
        headings = results[0].keys()
        f.write("\t".join(headings) + "\n")
        for result in results:
            f.write("\t".join([str(result[heading]) for heading in headings]) + "\n")

if __name__ == "__main__":
    main()
