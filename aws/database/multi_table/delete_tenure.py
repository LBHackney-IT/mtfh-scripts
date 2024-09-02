from mypy_boto3_dynamodb import ServiceResource
from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.opensearch.client.elasticsearch_client import LocalElasticsearchClient
from enums.enums import Stage
from mypy_boto3_dynamodb.service_resource import Table

from utils.confirm import confirm


def delete_tenure(tenure_id: str):
    # Setup
    DYNAMODB_FOUND = False
    ELASTICSEARCH_FOUND = False

    dynamo_service: ServiceResource = generate_aws_service(
        "dynamodb", Stage.HOUSING_PRODUCTION
    )
    tenure_table: Table = dynamo_service.Table("TenureInformation")

    es_client = LocalElasticsearchClient("tenures")

    # Fetch records
    tenure_record_dynamo = tenure_table.get_item(Key={"id": tenure_id})
    if "Item" in tenure_record_dynamo:
        print(f"\nTenure found in DynamoDB: {tenure_record_dynamo['Item']}")
        DYNAMODB_FOUND = True
    else:
        print("tenure not found in DynamoDB - skipping")
    assert "Item" in tenure_record_dynamo, "tenure not found in database"

    tenure_record_es = es_client.get(tenure_id)
    if tenure_record_es is not None:
        print(f"\nTenure found in Elasticsearch: {tenure_record_es}")
        ELASTICSEARCH_FOUND = True
    else:
        print("tenure not found in Elasticsearch - skipping")

    if DYNAMODB_FOUND and confirm(
        "\nAre you sure you want to delete this tenure from DynamoDB?"
    ):
        response = tenure_table.delete_item(
            Key={"id": tenure_record_dynamo["Item"]["id"]}
        )
        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), "tenure not deleted"

    if ELASTICSEARCH_FOUND and confirm(
        "\nAre you sure you want to delete this tenure from Elasticsearch?"
    ):
        es_client.delete(tenure_id)


if __name__ == "__main__":
    TENURE_ID = "90fb20a0-6322-6539-fe43-28b032bc9b4c"  # input("Tenure ID to delete: ")
    delete_tenure(TENURE_ID)
