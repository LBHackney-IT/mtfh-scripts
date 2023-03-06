import uuid

import boto3
import pytest
from moto import mock_dynamodb
import random

from src.utils.logger import Logger


@pytest.fixture
def use_mock_dynamodb():
    @mock_dynamodb
    def dynamodb_client():
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')

        # Create the table
        dynamodb.create_table(
            TableName="test-table",
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        return dynamodb

    return dynamodb_client


@pytest.fixture
def asset():
    return {
        "id": uuid.uuid4().hex,
        "assetId": str(random.randint(0, int(9e8))).zfill(8),
        "patches": [
            {
                "id": uuid.uuid4().hex,
                "responsibleEntities": [
                    {
                        "id": uuid.uuid4().hex,
                        "name": "Fake_FirstName Fake_LastName",
                        "personTypes": ["Responsible"],
                    }
                ],
            }
        ],
    }


@pytest.fixture
def person_patch_map():
    return [
        {
            "PatchId": uuid.uuid4().hex,
            "PersonId": uuid.uuid4().hex,
            "PersonName": "Fake_FirstName Fake_LastName",
            "PersonTypes": ["Housing Officer"],
        },
        # {
        #     "PatchId": uuid.uuid4().hex,
        #     "PersonId": uuid.uuid4().hex,
        #     "PersonName": "Fake_FirstName2 Fake_LastName2",
        #     "PersonTypes": ["Housing Officer"],
        # }
    ]