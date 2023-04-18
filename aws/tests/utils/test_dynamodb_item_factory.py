from dataclasses import dataclass
import uuid

from moto import mock_dynamodb

from src.database.utils.dynamodb_item_factory import DynamodbItemFactory
from src.utils.logger import Logger


@dataclass
class TestObject:
    id: str
    firstName: str
    surname: str
    placeOfBirth: str
    title: str
    dateOfBirth: str


@mock_dynamodb
class TestDynamodbTableTool:
    def test_dynamodb_asset_factory(self, use_mock_dynamodb):
        dynamodb_table = use_mock_dynamodb()
        mock_table = dynamodb_table.Table("test-table")
        # Arrange
        test_items = [{
            "id": uuid.uuid4().hex,
            "firstName": "John",
            "surname": "Smith",
            "placeOfBirth": "London",
            "title": "Mr",
            "dateOfBirth": "01/01/2000",
        },
            {
                "id": uuid.uuid4().hex,
                "firstName": "Jane",
                "surname": "Smith",
                "placeOfBirth": "London",
                "title": "Mrs",
                "dateOfBirth": "01/01/2000",
            }]

        expected = [
            TestObject(
                id=test_item["id"],
                firstName=test_item["firstName"],
                surname=test_item["surname"],
                placeOfBirth=test_item["placeOfBirth"],
                title=test_item["title"],
                dateOfBirth=test_item["dateOfBirth"]
            ) for test_item in test_items
        ]

        for item in test_items:
            mock_table.put_item(Item=item)

        # Act
        my_dynamo_tool = DynamodbItemFactory(mock_table, TestObject, Logger())
        actual = my_dynamo_tool.full_extract({"id": lambda x: bool(x)})

        # Assert
        assert actual == expected
