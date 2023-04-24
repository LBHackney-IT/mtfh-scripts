from boto3.dynamodb.conditions import Key
from mypy_boto3_dynamodb.service_resource import Table


def get_by_secondary_index(
        table: Table, index_name: str, secondary_key_name: str, secondary_key_value: str) -> list[dict]:
    response = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(secondary_key_name).eq(secondary_key_value)
    )
    return response.get("Items")
