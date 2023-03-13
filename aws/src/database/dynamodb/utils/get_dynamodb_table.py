# from mypy_boto3_dynamodb import ServiceResource
# from mypy_boto3_dynamodb.service_resource import Table
from difflib import get_close_matches

from aws.src.authentication.generate_aws_resource import generate_aws_resource
from aws.src.enums.enums import Stage


def get_dynamodb_table(table_name: str, stage: Stage) -> Table:
    """
    :param table_name: Name of the DynamoDB table to connect to (as on AWS)
    :param stage: A valid stage (e.g. Stage.DEVELOPMENT)
    :return: A boto3 DynamoDB table object
    """
    db_resource: ServiceResource = generate_aws_resource("dynamodb", stage)
    dynamo_table: Table = db_resource.Table(table_name)
    try:
        # Check that the table exists
        if dynamo_table.creation_date_time:
            print(f"Connected to DynamoDB table {table_name}")
    except dynamo_table.meta.client.exceptions.ResourceNotFoundException:
        valid_tables = [table.name for table in db_resource.tables.all()]
        raise ValueError(f"Did not find table named: {table_name}\n"
                         f"Valid tables: {valid_tables}\n"
                         f"Close matches: {get_close_matches(table_name, valid_tables)}")
    return dynamo_table
