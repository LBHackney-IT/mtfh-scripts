import uuid
from dataclasses import dataclass, asdict
from pathlib import Path

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import ResponsibleEntity, ResponsibleEntityContactDetails
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from enums.enums import Stage
from aws.src.database.domain.dynamo_domain_objects import Patch

from faker import Faker

from utils.confirm import confirm


class Config:
    STAGE = Stage.HOUSING_PRODUCTION
    logger = Logger("patch_reassignment")
    CSV_FILE = f"{Path(__file__).parent}/input/OfficerPatch.csv"


@dataclass
class PatchReassignment:
    patchId: str
    patchName: str
    officerName: str
    responsibleType: str
    emailAddress: str

    def __post_init__(self):
        self.patch_id = self.patchId.strip()
        self.patch_name = self.patchName.strip().upper()
        self.officer_name = self.officerName.strip()
        self.responsible_type = self.responsibleType.strip()
        assert self.responsible_type in ["HousingOfficer", "HousingAreaManager"]
        self.email_address = self.emailAddress.strip().lower()


def set_responsible_entities(patches_table: Table, patch_reassignment: PatchReassignment, patch: Patch):
    if patch.versionNumber is None:
        patch.versionNumber = 0
    else:
        patch.versionNumber += 1

    assigned_entities = [entity for entity in patch.responsibleEntities]
    # assert len(assigned_entities) > 0, f"Patch {patch.id} has no assigned entities"
    # assigned_entity = assigned_entities[0]

    if len(assigned_entities) == 0:
        entity_id = str(uuid.uuid4())
    else:
        entity_id = assigned_entities[0].id

    responsible_entity = ResponsibleEntity(
        id=entity_id,
        name=patch_reassignment.officer_name,
        contactDetails=ResponsibleEntityContactDetails(patch_reassignment.email_address),
        responsibleType=patch_reassignment.responsible_type,
    )

    if responsible_entity.name == "":
        responsible_entity.name = None
    if responsible_entity.contactDetails.emailAddress == "":
        responsible_entity.contactDetails.emailAddress = None

    Config.logger.log(f"New data for patch {patch.id}: {asdict(responsible_entity)}")

    patches_table.update_item(
        Key={"id": patch_reassignment.patch_id},
        UpdateExpression=f"SET responsibleEntities = :r, versionNumber = :v",
        ExpressionAttributeValues={":r": [asdict(responsible_entity)], ":v": patch.versionNumber},
        ReturnValues="NONE",
    )


def main():
    table = get_dynamodb_table("PatchesAndAreas", Config.STAGE)
    patches_raw = table.scan()["Items"]
    patches = [Patch.from_data(patch) for patch in patches_raw if patch["name"] != "Hackney"]

    if Config.STAGE in [Stage.HOUSING_DEVELOPMENT, Stage.HOUSING_STAGING]:
        fake = Faker()
        for patch in patches:
            first_name = fake.first_name()
            last_name = fake.last_name()
            email_address = f"{first_name}.{last_name}@hackney.gov.uk"
            
            # Assign the E2E testing account to HN10 for the frontend tests
            if patch.name == "HN10":
                first_name = "E2E"
                last_name = "Tester"
                email_address = f"e2e-testing-{Config.STAGE.to_env_name()}-t-and-l@hackney.gov.uk"

            reassignment = PatchReassignment(
                patchId=patch.id,
                patchName=patch.name,
                responsibleType="HousingOfficer" if patch.patchType.strip().lower() == "patch" else "HousingAreaManager",
                officerName=f"FAKE_{first_name} FAKE_{last_name}",
                emailAddress=email_address
            )
            set_responsible_entities(table, reassignment, patch)

    elif Config.STAGE == Stage.HOUSING_PRODUCTION:
        if not confirm("Are you sure you want to reassign patches in PRODUCTION?"):
            return
        patch_reassignments_raw = csv_to_dict_list(Config.CSV_FILE)
        patch_reassignments = [PatchReassignment(**assignment) for assignment in patch_reassignments_raw]
        for reassignment in patch_reassignments:
            patch = [patch for patch in patches if patch.id == reassignment.patch_id][0]
            set_responsible_entities(table, reassignment, patch)


if __name__ == "__main__":
    main()
