from dataclasses import dataclass, asdict

from aws.src.database.domain.dynamo_domain_objects import Patch
from aws.src.database.domain.elasticsearch.staff import StaffMemberEs
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from enums.enums import Stage

from es_check import LocalElasticsearchClient


@dataclass
class Config:
    TABLE_NAME = "PatchesAndAreas"
    OUTPUT_CLASS = Patch
    STAGE = Stage.HOUSING_STAGING
    LOGGER = Logger()

def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)

    patch_id = "e611b1af-e2cc-45ee-8f17-cd8c09307121"  # SH1
    patch_raw = table.query(KeyConditionExpression="id = :id", ExpressionAttributeValues={":id": patch_id})["Items"]
    patch = Patch.from_data(patch_raw[0])
    print(patch)

    es_client = LocalElasticsearchClient(3333, "processes")

    process_id = ""
    process = es_client.query({"match": {"id": process_id}})[0]

    process["_source"]["patchAssignment"] = {
        'patchId': 'e611b1af-e2cc-45ee-8f17-cd8c09307121',
        'patchName': 'SH1',
        'responsibleEntityId': '0a8c26b4-315f-4027-8b74-11822f0fddd2',
        'responsibleName': 'Jeff Johnson'
    }

    es_client.update(process_id, process["_source"])


if __name__ == "__main__":
    main()
