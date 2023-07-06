import datetime
import json
from pprint import pprint

import pyperclip
from dateutil import parser
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

# Config - change these
STAGE = Stage.HOUSING_PRODUCTION
STAGE_PARAM = "production"  # Stage parameter for SSM

# For modifying start/end dates of a tenure
TENURE_ID = input(f"{STAGE_PARAM.upper()} tenure ID to update: ").strip()  # Do not hardcode this

PARAM_KEY_ES = ""
while PARAM_KEY_ES not in ["startOfTenureDate", "endOfTenureDate"]:
    start_or_end = input("Update start (s) or end (e) date for tenure?").strip().lower()
    if start_or_end in ["s", "start"]:
        PARAM_KEY_ES = "startOfTenureDate"
    if start_or_end in ["e", "end"]:
        PARAM_KEY_ES = "endOfTenureDate"

UPDATE_DATE = parser.parse(timestr=input("Enter a date: ").strip(),
                           parserinfo=parser.parserinfo(dayfirst=True)).isoformat()
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
    _confirm("Correct?")
    tenure = get_tenure_dynamodb(TENURE_ID)
    connect_to_jumpbox_for_es(instance_id=INSTANCE_ID, stage=STAGE.value)
    # update_tenure_elasticsearch(tenure_pk=TENURE_ID)
    # update_property_elasticsearch(property_pk=tenure["tenuredAsset"]["id"])
    # update_tenure_dynamodb(tenure)
    # update_property_dynamodb(tenure)
    # update_persons_dynamodb(tenure)


def connect_to_jumpbox_for_es(instance_id=INSTANCE_ID, stage=STAGE.value):
    command = f"aws ssm start-session --target {instance_id} --region eu-west-2 --profile {stage};\n"
    pyperclip.copy(command)

    print(
        "\n== Connect to Jumpbox/Bastion for ElasticSearch ==\n",
        command,
        "\n== ==\n")
    _confirm(f"Command to connect to Jumpbox/Bastion copied to clipboard. Connected?")


def query_item_by_pk_es(query_path: str, es_extension: str, primary_key: str):
    with open(query_path, "r") as outfile:
        es_query_cmd = outfile.read()
    es_query = json.dumps({"query": {"term": {"id.keyword": primary_key}}})
    curl_query = f"curl -X GET '{ES_DOMAIN}/{es_extension}/_search' -H 'Content-Type: application/json' " \
                 f"-d '{es_query}' | python -c '\n{es_query_cmd}'"
    print("== Elasticsearch Query ==")
    print(curl_query)
    pyperclip.copy(curl_query)
    _confirm(f"Command to query ES copied to clipboard. Correct {es_extension}? ")


def update_item_by_pk_es(es_extension: str, primary_key: str, update_obj: str):
    curl_update = f"curl -X POST '{ES_DOMAIN}/{es_extension}/_update/{primary_key}'" \
                  f" -H 'Content-Type: application/json' -d '{update_obj}'"
    pyperclip.copy(curl_update)

    print(f"== {es_extension} Elasticsearch Update ==")
    print(update_obj)
    _confirm(f"Command to update {es_extension[0:-1]} copied to clipboard.\nElasticsearch updated?")

    print(f"Elasticsearch updated!")


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
    query_item_by_pk_es("input/elasticsearch_query_tenure.py", index, tenure_pk)
    update_item_by_pk_es(index, tenure_pk, update_obj)


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
    query_item_by_pk_es("input/elasticsearch_query_asset.py", index, property_pk)
    update_item_by_pk_es(index, property_pk, update_obj)


def get_tenure_dynamodb(primary_key=TENURE_ID) -> dict:
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    tenure_item: dict = tenure_table.get_item(Key={"id": primary_key})["Item"]
    tenure_item[PARAM_KEY_DYNAMO_TENURE] = UPDATE_DATE

    return tenure_item


def update_tenure_dynamodb(tenure_item: dict):
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    print("\n== DynamoDB Item ==")
    pprint(tenure_item)
    print("== DynamoDB Item ==")
    print(f"Pending Update: {PARAM_KEY_DYNAMO_TENURE}: {tenure_item[PARAM_KEY_DYNAMO_TENURE]} -> {UPDATE_DATE}")

    _confirm("Confirm updating this tenure?")

    tenure_id = tenure_item["id"]
    tenure_table.update_item(
        Key={"id": tenure_id},
        UpdateExpression=f"set {PARAM_KEY_DYNAMO_TENURE} = :r",
        ExpressionAttributeValues={
            ":r": UPDATE_DATE
        },
        ReturnValues="UPDATED_NEW"
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

    _confirm(f"Confirm updating this property's tenure {PARAM_KEY_DYNAMO_ASSET}?")

    property_tenure[PARAM_KEY_DYNAMO_ASSET] = UPDATE_DATE
    asset_table.update_item(
        Key={"id": property_id},
        UpdateExpression=f"SET tenure = :r",
        ExpressionAttributeValues={
            ":r": property_tenure
        },
        ReturnValues="UPDATED_NEW"
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
                    f"{tenure.get(PARAM_KEY_DYNAMO_PERSON).split('T')[0]} -> {UPDATE_DATE.split('T')[0]}"
                )
                person_tenures[i][PARAM_KEY_DYNAMO_PERSON] = UPDATE_DATE.split('T')[0]
                print(tenure['assetFullAddress'])
        print(f"Update tenure for person {tenure_person['id']}, {tenure_person['fullName']}?")

        if _confirm(f"Confirm updating this person's tenure {PARAM_KEY_DYNAMO_PERSON}?", kill=False):
            persons_table.update_item(
                Key={"id": tenure_person['id']},
                UpdateExpression=f"set tenures = :r",
                ExpressionAttributeValues={
                    ":r": person_tenures
                },
                ReturnValues="UPDATED_NEW"
            )


def _confirm(question: str, kill=True):
    confirm = input(f"{question} (y/n): ").lower() not in ["y", "yes"]
    if confirm and kill:
        print("Exiting")
        exit()
    if confirm:  # No kill / exit
        return False
    return True


if __name__ == "__main__":
    main()
