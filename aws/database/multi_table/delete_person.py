from mypy_boto3_dynamodb import ServiceResource
from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.opensearch.client.elasticsearch_client import LocalElasticsearchClient
from enums.enums import Stage
from mypy_boto3_dynamodb.service_resource import Table

from utils.confirm import confirm


def delete_person(person_id: str):
    # Setup
    DYNAMODB_FOUND = False
    ELASTICSEARCH_FOUND = False

    dynamo_service: ServiceResource = generate_aws_service(
        "dynamodb", Stage.HOUSING_PRODUCTION
    )
    person_table: Table = dynamo_service.Table("Persons")

    es_client = LocalElasticsearchClient("persons")

    # Fetch records
    person_record_dynamo = person_table.get_item(Key={"id": person_id})
    if "Item" in person_record_dynamo:
        print(f"Person found in DynamoDB: {person_record_dynamo['Item']}")
        DYNAMODB_FOUND = True
    else:
        print("Person not found in DynamoDB - skipping")
    assert "Item" in person_record_dynamo, "Person not found in database"

    person_record_es = es_client.get(person_id)
    if person_record_es is not None:
        print(f"Person found in Elasticsearch: {person_record_es}")
        ELASTICSEARCH_FOUND = True
    else:
        print("Person not found in Elasticsearch - skipping")

    if DYNAMODB_FOUND and confirm("Are you sure you want to delete this person?"):
        response = person_table.delete_item(
            Key={"id": person_record_dynamo["Item"]["id"]}
        )
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "Person not deleted"

    if ELASTICSEARCH_FOUND and confirm(
        "Are you sure you want to delete this person from Elasticsearch?"
    ):
        es_client.delete(person_id)


if __name__ == "__main__":
    delete_person(input("Person ID to delete: "))
