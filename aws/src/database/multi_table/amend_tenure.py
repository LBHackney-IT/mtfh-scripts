import json
from pprint import pprint

from dateutil import parser
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.authentication.generate_aws_resource import generate_aws_service
from aws.src.utils.confirm import confirm
from aws.src.database.multi_table.utils.amend_tenure_utils import query_item_by_pk_es, update_item_by_pk_es, \
    connect_to_jumpbox_for_es
from enums.enums import Stage

# Config - change these
STAGE = Stage.HOUSING_STAGING
STAGE_PARAM = "staging"  # Stage parameter for SSM

# For modifying start/end dates of a tenure
TENURE_ID = input(f"{STAGE_PARAM.upper()} tenure ID to update: ").strip()  # Do not hardcode this

PARAM_KEY_ES = ""
while PARAM_KEY_ES not in ["startOfTenureDate", "endOfTenureDate"]:
    start_or_end = input("Update start (s) or end (e) date for tenure? ").strip().lower()
    if start_or_end in ["s", "start"]:
        PARAM_KEY_ES = "startOfTenureDate"
    if start_or_end in ["e", "end"]:
        PARAM_KEY_ES = "endOfTenureDate"

_update_date = input("Enter a date (e.g. DD/MM/YYYY or YYYY-MM-DD) or enter n or null to unset: ").strip()
if _update_date.lower() in ["n", "null"]:
    UPDATE_DATE = None
    print("Unsetting date")
else:
    UPDATE_DATE = parser.parse(timestr=_update_date, parserinfo=parser.parserinfo(dayfirst=True)).isoformat()
PARAM_KEY_DYNAMO_TENURE = PARAM_KEY_ES
PARAM_KEY_DYNAMO_ASSET = PARAM_KEY_ES
PARAM_KEY_DYNAMO_PERSON = "startDate" if PARAM_KEY_ES == "startOfTenureDate" else "endDate"

# Parameter store paths
_ES_DOMAIN_PARAM_PATH = f"/housing-search-api/{STAGE_PARAM}/elasticsearch-domain"
_JUMP_BOX_INSTANCE_NAME_PATH = "platform-apis-jump-box-instance-name"

# Parameters
ssm_client = generate_aws_service("ssm", STAGE, "client")
ES_DOMAIN = ssm_client.get_parameter(Name=_ES_DOMAIN_PARAM_PATH)["Parameter"]["Value"]
INSTANCE_ID = ssm_client.get_parameter(Name=_JUMP_BOX_INSTANCE_NAME_PATH)["Parameter"]["Value"]

PROPERTY_TABLE_NAME = "Assets"
TENURE_TABLE_NAME = "TenureInformation"
PERSONS_TABLE_NAME = "Persons"


def main():
    print("\n== Update ==")
    print(f"Tenure ID: {TENURE_ID}")
    print(f"Param key: {PARAM_KEY_ES}")
    print(f"New value: {UPDATE_DATE}")
    confirm("Correct?")

    tenure = get_tenure_dynamodb(TENURE_ID)
    original_date = tenure.get(PARAM_KEY_DYNAMO_TENURE)

    update_dynamodb = confirm(f"Update {STAGE.value} DynamoDB?", kill=False)
    if update_dynamodb:
        update_tenure_dynamodb(tenure)
        update_property_dynamodb(tenure)
        update_persons_dynamodb(tenure)

    update_es = confirm(f"Update {STAGE.value} Elasticsearch?", kill=False)
    if update_es:
        connect_to_jumpbox_for_es(instance_id=INSTANCE_ID, stage=STAGE.value)
        update_tenure_elasticsearch(tenure_pk=TENURE_ID)
        update_property_elasticsearch(property_pk=tenure["tenuredAsset"]["id"])

    final_message(tenure, original_date)


def final_message(tenure: dict[str, str | None], original_date: str | None):
    human_date = parser.parse(timestr=original_date).strftime("%d/%m/%Y") if original_date is not None else "no date"
    human_update_date = parser.parse(timestr=UPDATE_DATE).strftime("%d/%m/%Y") if UPDATE_DATE is not None else "no date"
    human_start_or_end = "start date" if PARAM_KEY_ES == "startOfTenureDate" else "end date"
    print(
        f"===\n"
        f"The tenure TPR {tenure['paymentReference']}'s {human_start_or_end} has been updated "
        f"from {human_date} to {human_update_date}.\n"
        f"https://manage-my-home.hackney.gov.uk/tenure/{tenure['id']}\n\n"
        f"You may need to clear your browser cache (all time) to see the changes immediately, see here for info:"
        f" https://support.google.com/accounts/answer/32050\n"
        f"Please respond to reopen the case if you notice any issues."
    )


def update_tenure_elasticsearch(tenure_pk=TENURE_ID):
    # Update tenure.startOfTenureDate or tenure.endOfTenureDate
    index = "tenures"
    update_obj = json.dumps(
        {
            "doc": {
                PARAM_KEY_ES: UPDATE_DATE
            }
        }
    )
    query_item_by_pk_es("input/elasticsearch_query_tenure.py", ES_DOMAIN, index, tenure_pk)
    update_item_by_pk_es(ES_DOMAIN, index, tenure_pk, update_obj)


def update_property_elasticsearch(property_pk):
    # Update property.tenure.startOfTenureDate or property.tenure.endOfTenureDate
    index = "assets"
    update_obj = json.dumps(
        {
            "doc": {
                "tenure": {
                    PARAM_KEY_ES: UPDATE_DATE
                }
            }
        }
    )
    query_item_by_pk_es("input/elasticsearch_query_asset.py", ES_DOMAIN, index, property_pk)
    update_item_by_pk_es(ES_DOMAIN, index, property_pk, update_obj)


def get_tenure_dynamodb(primary_key=TENURE_ID) -> dict:
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    tenure_item: dict = tenure_table.get_item(Key={"id": primary_key})["Item"]
    return tenure_item


def update_tenure_dynamodb(tenure_item: dict):
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    print("\n== DynamoDB Item ==")
    pprint(tenure_item)
    print("== DynamoDB Item ==")
    print(f"Pending Update: {PARAM_KEY_DYNAMO_TENURE}: {tenure_item.get(PARAM_KEY_DYNAMO_TENURE)} -> {UPDATE_DATE}")

    if not confirm("Confirm updating this tenure?", kill=False):
        print("Tenure not updated.")
        return

    tenure_id = tenure_item["id"]
    tenure_table.update_item(
        Key={"id": tenure_id},
        UpdateExpression=f"set {PARAM_KEY_DYNAMO_TENURE} = :r",
        ExpressionAttributeValues={":r": UPDATE_DATE},
        ReturnValues="UPDATED_NEW",
    )


def update_property_dynamodb(tenure_item: dict):
    asset_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(PROPERTY_TABLE_NAME)
    property_id: str = tenure_item["tenuredAsset"]["id"]
    property_item: dict = asset_table.get_item(Key={"id": property_id})["Item"]
    property_tenure = property_item["tenure"]

    pprint(property_item["id"])
    pprint(property_item["assetAddress"]["addressLine1"])
    pprint(property_tenure)
    print(f"Pending Update: tenure {PARAM_KEY_DYNAMO_ASSET}: "
          f"{property_item['tenure'].get(PARAM_KEY_DYNAMO_ASSET)} -> {UPDATE_DATE}")

    if not confirm(f"Confirm updating this property's tenure {PARAM_KEY_DYNAMO_ASSET}?", kill=False):
        print("Tenure not updated.")
        return

    property_tenure[PARAM_KEY_DYNAMO_ASSET] = UPDATE_DATE
    asset_table.update_item(
        Key={"id": property_id},
        UpdateExpression=f"SET tenure = :r",
        ExpressionAttributeValues={":r": property_tenure},
        ReturnValues="UPDATED_NEW",
    )


def update_persons_dynamodb(tenure_item: dict):
    persons_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(PERSONS_TABLE_NAME)
    tenure_id = tenure_item["id"]

    household_members: list[dict] = tenure_item["householdMembers"]
    if len(household_members) == 0:
        print(f"No household members found in tenure {tenure_id}")
        return

    for tenure_person in household_members:
        person_item: dict = persons_table.get_item(Key={"id": tenure_person['id']})["Item"]

        person_tenures: list[dict] = person_item["tenures"]
        for i, tenure in enumerate(person_tenures):
            if tenure["id"] == tenure_id:
                pprint(person_item)
                print("== DynamoDB Item ==")
                print(
                    f"Pending Update: tenure {PARAM_KEY_DYNAMO_PERSON}: "
                    f"{str(tenure.get(PARAM_KEY_DYNAMO_PERSON)).split('T')[0]} -> {UPDATE_DATE.split('T')[0]}"
                )
                person_tenures[i][PARAM_KEY_DYNAMO_PERSON] = str(UPDATE_DATE).split("T")[0]
                print(tenure["assetFullAddress"])
        print(f"Update tenure for person {tenure_person['id']}, {tenure_person['fullName']}?")

        if confirm(f"Confirm updating this person's tenure {PARAM_KEY_DYNAMO_PERSON}?", kill=False):
            persons_table.update_item(
                Key={"id": tenure_person["id"]},
                UpdateExpression=f"set tenures = :r",
                ExpressionAttributeValues={":r": person_tenures},
                ReturnValues="UPDATED_NEW",
            )


if __name__ == "__main__":
    main()
