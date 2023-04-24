import json
from datetime import datetime
from pprint import pprint

import pyperclip
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

# Config - change these
STAGE = Stage.HOUSING_PRODUCTION
STAGE_PARAM = "production"  # Stage parameter for SSM
ES_EXTENSION = "tenures"
_update_date = datetime(day=31, month=1, year=2022).isoformat()
NEW_PARAM_VALUE = _update_date  # _update_date

# For modifying start/end dates of a tenure
TENURE_ID = input("Enter a tenure ID to update: ").strip()  # Do not hardcode this
PARAM_KEY_ES = "startOfTenureDate"  # "startOfTenureDate" or "endOfTenureDate"
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

ELASTICSEARCH_UPDATE_OBJ = json.dumps(
    {
        "doc": {
            PARAM_KEY_ES: NEW_PARAM_VALUE
        }
    }
)

PROPERTY_TABLE_NAME = "Assets"
TENURE_TABLE_NAME = "TenureInformation"
PERSONS_TABLE_NAME = "Persons"

with open("input/elasticsearch_query_tenure.py", "r") as outfile:
    ES_QUERY_CMD = outfile.read()


def main():
    print("== Update ==")
    print(f"Tenure ID: {TENURE_ID}")
    print(f"Param key: {PARAM_KEY_ES}")
    print(f"New value: {NEW_PARAM_VALUE}")
    tenure = get_tenure_dynamodb()
    # update_elasticsearch()
    # update_tenure_dynamodb(tenure)
    update_property_dynamodb(tenure)
    # update_person_dynamodb(tenure)


def update_elasticsearch(
        instance_id=INSTANCE_ID, es_domain=ES_DOMAIN, es_extension=ES_EXTENSION,
        primary_key=TENURE_ID, update_obj=ELASTICSEARCH_UPDATE_OBJ, es_query_cmd=ES_QUERY_CMD):
    command = f"aws ssm start-session --target {instance_id} --region eu-west-2 --profile {STAGE.value};\n"
    pyperclip.copy(command)

    print(
        "\n== Connect to Jumpbox/Bastion for ElasticSearch ==\n",
        command,
        "\n== ==\n")
    _confirm(f"Command to connect to Jumpbox/Bastion copied to clipboard. Connected?")

    es_query = json.dumps({"query": {"term": {"id.keyword": primary_key}}})
    curl_query = f"curl -X GET '{es_domain}/{es_extension}/_search' -H 'Content-Type: application/json' " \
                 f"-d '{es_query}' | python -c '\n{es_query_cmd}'"
    print("== Elasticsearch Query ==")
    print(curl_query)
    pyperclip.copy(curl_query)
    _confirm(f"Command to query ES copied to clipboard. Correct {es_extension}? ")

    curl_update = f"curl -X POST '{es_domain}/{es_extension}/_update/{primary_key}' -H 'Content-Type: application/json' -d '{update_obj}'"
    pyperclip.copy(curl_update)

    _confirm(f"Command to update elasticsearch {es_extension} copied to clipboard.\nElasticsearch updated?")

    print(f"Elasticsearch updated! Verify with: \n{curl_query}")


def get_tenure_dynamodb(primary_key=TENURE_ID) -> dict:
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    tenure_item: dict = tenure_table.get_item(Key={"id": primary_key})["Item"]
    tenure_item[PARAM_KEY_DYNAMO_TENURE] = NEW_PARAM_VALUE

    return tenure_item


def update_tenure_dynamodb(tenure_item: dict):
    tenure_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(TENURE_TABLE_NAME)
    print("\n== DynamoDB Item ==")
    pprint(tenure_item)
    print("== DynamoDB Item ==")
    print(f"Pending Update: {PARAM_KEY_DYNAMO_TENURE}: {tenure_item[PARAM_KEY_DYNAMO_TENURE]} -> {NEW_PARAM_VALUE}")

    _confirm("Confirm updating this tenure?")

    tenure_id = tenure_item["id"]
    tenure_table.update_item(
        Key={"id": tenure_id},
        UpdateExpression=f"set {PARAM_KEY_DYNAMO_TENURE} = :r",
        ExpressionAttributeValues={
            ":r": NEW_PARAM_VALUE
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
          f"{property_item.get(PARAM_KEY_DYNAMO_ASSET)} -> {NEW_PARAM_VALUE}")

    _confirm(f"Confirm updating this property's tenure {PARAM_KEY_DYNAMO_ASSET}?")

    property_tenure[PARAM_KEY_DYNAMO_ASSET] = NEW_PARAM_VALUE
    asset_table.update_item(
        Key={"id": property_id},
        UpdateExpression=f"SET tenure = :r",
        ExpressionAttributeValues={
            ":r": property_tenure
        },
        ReturnValues="UPDATED_NEW"
    )


def update_person_dynamodb(tenure_item: dict):
    persons_table: Table = generate_aws_service("dynamodb", STAGE, "resource").Table(PERSONS_TABLE_NAME)
    tenure_id = tenure_item["id"]

    household_members: list[dict] = tenure_item["householdMembers"]
    if len(household_members) == 1:
        tenure_person = household_members[0]
    elif len(household_members) > 1:
        print(f"Household members: {[(i, person['fullName']) for i, person in enumerate(household_members)]}")
        index_to_update = int(input("Enter index of person to update: "))
        tenure_person = household_members[index_to_update]
    else:
        raise ValueError(f"No household members found in tenure {tenure_id}")
    _confirm(f"Update tenure for person {tenure_person['id']}, {tenure_person['fullName']}?")

    person_item: dict = persons_table.get_item(Key={"id": tenure_person['id']})["Item"]

    person_tenures: list[dict] = person_item["tenures"]
    for i, tenure in enumerate(person_tenures):
        if tenure["id"] == tenure_id:
            pprint(person_item)
            print("== DynamoDB Item ==")
            print(
                f"Pending Update: tenure {PARAM_KEY_DYNAMO_PERSON}: {tenure.get(PARAM_KEY_DYNAMO_PERSON)} -> {NEW_PARAM_VALUE}")
            person_tenures[i][PARAM_KEY_DYNAMO_PERSON] = NEW_PARAM_VALUE

    _confirm(f"Confirm updating this person's tenure {PARAM_KEY_DYNAMO_PERSON}?")

    persons_table.update_item(
        Key={"id": tenure_person['id']},
        UpdateExpression=f"set tenures = :r",
        ExpressionAttributeValues={
            ":r": person_tenures
        },
        ReturnValues="UPDATED_NEW"
    )


def _confirm(question):
    confirm = input(f"{question} (y/n): ")
    if confirm.lower() not in ["y", "yes"]:
        print("Exiting")
        exit()


if __name__ == "__main__":
    main()
