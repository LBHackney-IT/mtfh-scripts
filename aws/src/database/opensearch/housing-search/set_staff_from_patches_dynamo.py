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
    STAGE = Stage.HOU
    E2E_PATCH = "SH6"
    LOGGER = Logger()


def set_es_from_patch(es_client: LocalElasticsearchClient, patch: Patch, all_patches: list[Patch]):
    staff_member = StaffMemberEs.from_patch(patch, all_patches)
    all_staff_es_raw = es_client.match_all()
    all_staff_es: list[StaffMemberEs] = []
    for staff in all_staff_es_raw:
        all_staff_es.append(StaffMemberEs.from_es(staff))

    # Check if staff member already exists
    staff_member_exists = False
    for staff in all_staff_es:
        if staff.patchId == staff_member.patchId:
            staff_member_exists = True
            break

    if staff_member_exists:
        # Delete and re-create
        member = es_client.query({"match": {"patchId": staff_member.patchId}})[0]
        es_client.delete(member.get("_id"))
        es_client.index(staff_member.patchId, asdict(staff_member))
        Config.LOGGER.log(f"Deleted and re-created staff member {staff_member.patchId} {staff_member.firstName} {staff_member.lastName}")
    else:
        # Create new staff member
        es_client.index(staff_member.patchId, asdict(staff_member))
        Config.LOGGER.log(f"Created new staff member {staff_member.patchId} {staff_member.firstName} {staff_member.lastName}")

    # Verify new staff member
    new_member = es_client.get(staff_member.patchId)
    assert new_member["_source"]["firstName"] == staff_member.firstName


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)

    patches_raw = table.scan()["Items"]
    all_patches = [Patch.from_data(patch_raw) for patch_raw in patches_raw]

    es_client = LocalElasticsearchClient(3333, "asdfas")

    patch = all_patches[0]
    set_es_from_patch(es_client, patch, all_patches)


if __name__ == "__main__":
    main()
