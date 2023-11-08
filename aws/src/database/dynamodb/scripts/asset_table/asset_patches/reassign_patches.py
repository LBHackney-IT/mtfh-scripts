from dataclasses import asdict, dataclass
from pathlib import Path

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import ResponsibleEntity, ResponsibleEntityContactDetails, _dataclass_from_data
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from enums.enums import Stage
from aws.src.database.domain.dynamo_domain_objects import Patch

from faker import Faker
fake = Faker()

STAGE = Stage.HOUSING_DEVELOPMENT
logger = Logger("patch_reassignment")


@dataclass
class PatchReassignment:
    id: str
    patchName: str | None
    officerName: str | None
    emailAddress: str | None
    
    @classmethod
    def from_data(cls, data):
        return _dataclass_from_data(cls, data)
    
    
def set_responsible_entities(patches_table: Table, patch_reassignment: PatchReassignment, patch: Patch):
    if not patch.versionNumber:
        patch.versionNumber = 0
    else:
        patch.versionNumber += 1
    
    assert patch.patchType in ["patch", "area"], f"Patch {patch.id} has patchType {patch.patchType}"
    responsible_type = "HousingOfficer" if patch.patchType == "patch" else "HousingAreaManager"

    logger.log(f"Setting responsible entity for {patch.id} {patch.name} to {patch_reassignment.officerName} ({patch_reassignment.emailAddress})")

    responsible_entity = asdict(
        ResponsibleEntity(
            id = patch_reassignment.id,
            name = patch_reassignment.officerName,
            contactDetails= ResponsibleEntityContactDetails(patch_reassignment.emailAddress),
            responsibleType=responsible_type
        )
    )
    
    new_responsible_entities = [responsible_entity]

    patches_table.update_item(
        Key={"id": patch_reassignment.id},
        UpdateExpression=f"SET responsibleEntities = :r, versionNumber = :v",
        ExpressionAttributeValues={":r": new_responsible_entities, ":v": patch.versionNumber},
        ReturnValues="NONE",
        )


def main():
    table = get_dynamodb_table("PatchesAndAreas", STAGE)
    patches_raw = table.scan()["Items"]
    patches = [Patch.from_data(patch) for patch in patches_raw]

    if STAGE in [Stage.HOUSING_DEVELOPMENT, Stage.HOUSING_STAGING]:
        for patch in patches:
            fake_name = fake.name()
            fake_email = f"{fake_name.replace(' ', '.')}@hackney.gov.uk"
            reassignment = PatchReassignment(
                id = patch.id,
                patchName = patch.name,
                officerName = fake_name,
                emailAddress = fake_email
            )
            set_responsible_entities(table, reassignment, patch)

    elif STAGE == Stage.HOUSING_PRODUCTION:
        print("Updating data in PROD")
        WORKDIR = Path(__file__).parent  # gets current directory *even when run as module*
        CSV_FILE = f"{WORKDIR}/input/OfficerPatch.csv"  # e.g. Patch-update-Development.csv
        patch_reassignments_raw = csv_to_dict_list(CSV_FILE)
        patch_reassignments = [PatchReassignment.from_data(patch) for patch in patch_reassignments_raw]
        for reassignment in patch_reassignments:
            patch = [patch for patch in patches if patch.id == reassignment.id][0]
            set_responsible_entities(table, reassignment, patch)

if __name__ == "__main__":
    main()