from dataclasses import asdict, dataclass
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


@dataclass
class PatchReassignment:
    patch_id: str
    patch_name: str
    officer_name: str
    responsible_type: str
    email_address: str

    def __post_init__(self):
        self.patch_id = self.patch_id.strip()
        self.patch_name = self.patch_name.strip().upper()
        self.officer_name = self.officer_name.strip()
        self.responsible_type = self.responsible_type.strip()
        assert self.responsible_type in ["HousingOfficer", "HousingAreaManager"]
        self.email_address = self.email_address.strip().lower()


def set_responsible_entities(patches_table: Table, patch_reassignment: PatchReassignment, patch: Patch):
    if patch.versionNumber is None:
        patch.versionNumber = 0
    else:
        patch.versionNumber += 1

    assigned_entities = [entity for entity in patch.responsibleEntities]
    assert len(assigned_entities) > 0, f"Patch {patch.id} has no assigned entities"
    assigned_entity = assigned_entities[0]

    Config.logger.log(f"Setting responsible entity for, {patch.id}, {patch.name}, "
                      f"{patch_reassignment.officer_name}, {patch_reassignment.email_address}")

    responsible_entity = ResponsibleEntity(
        id=assigned_entity.id,
        name=patch_reassignment.officer_name,
        contactDetails=ResponsibleEntityContactDetails(patch_reassignment.email_address),
        responsibleType=patch_reassignment.responsible_type,
    )

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
            reassignment = PatchReassignment(
                patch_id=patch.id,
                patch_name=patch.name,
                responsible_type="HousingOfficer" if patch.patchType.strip().lower() == "patch" else "HousingAreaManager",
                officer_name=f"FAKE_{first_name} FAKE_{last_name}",
                email_address=f"{first_name}.{last_name}@hackney.gov.uk"
            )
            set_responsible_entities(table, reassignment, patch)

    elif Config.STAGE == Stage.HOUSING_PRODUCTION:
        if not confirm("Are you sure you want to reassign patches in PRODUCTION?"):
            return
        WORKDIR = Path(__file__).parent
        CSV_FILE = f"{WORKDIR}/input/OfficerPatch.csv"
        patch_reassignments_raw = csv_to_dict_list(CSV_FILE)
        patch_reassignments = [PatchReassignment(**assignment) for assignment in patch_reassignments_raw]
        for reassignment in patch_reassignments:
            patch = [patch for patch in patches if patch.id == reassignment.patch_id][0]
            set_responsible_entities(table, reassignment, patch)


if __name__ == "__main__":
    main()
