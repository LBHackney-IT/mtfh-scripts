from dataclasses import asdict

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import ResponsibleEntity, ResponsibleEntityContactDetails
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from enums.enums import Stage
from aws.src.database.domain.dynamo_domain_objects import Patch

from faker import Faker


def link_patches_to_areas(table: Table):
    """
    Set patch ParentIds to the correct area and update the PatchesAndAreas table
    """
    patches_raw = table.scan()["Items"]
    patches = [Patch.from_data(patch) for patch in patches_raw]

    areas = [item for item in patches if item.patchType == "area" and item.name != "Hackney"]
    assert len(areas) == 7, [area.name for area in areas]
    patches = [item for item in patches if item.patchType == "patch"]
    assert len(patches) >= 46, ([patch.name for patch in patches], len(patches))

    # HN1 and HN2 areas correspond with these specific patches
    hn1_patch_names = [f"HN{i}" for i in [2, 5, 8, 9, 10, 11]]
    hn2_patch_names = [f"HN{i}" for i in [1, 3, 4, 6, 7]]

    for area in areas:
        if "HN1" in area.name:
            area_patches = [patch for patch in patches if patch.name in hn1_patch_names]
        elif "HN2" in area.name:
            area_patches = [patch for patch in patches if patch.name in hn2_patch_names]
        else:
            area_code = area.name.split(" ")[0]
            area_patches = [patch for patch in patches if patch.name.startswith(area_code)]

        assert len(area_patches) > 0, f"Area {area.name} has no patches"
        for patch in area_patches:
            patch.parentId = area.patch_id
            patch.versionNumber += 1
            table.put_item(Item=asdict(patch))
            print(f"Updated {patch.name} to link to {area.name}")


def set_fake_responsible_entities(table: Table):
    """
    Create fake responsible entities for patches and areas and update the PatchesAndAreas table
    Use to create valid dummy data on development/staging
    """
    patches_raw = table.scan()["Items"]
    patches_and_areas = [Patch.from_data(patch) for patch in patches_raw]

    fake = Faker()

    for patch in patches_and_areas:
        try:
            current_id = patch.patch_id
            first_name: str = fake.first_name()
            last_name: str = fake.last_name()
            responsibleType = "HousingOfficer" if patch.patchType == "patch" else "HousingAreaManager"
            contact_details = ResponsibleEntityContactDetails(
                emailAddress=f"{first_name.lower()}.{last_name.lower()}@hackney.gov.uk"
            )

            responsible_entity = ResponsibleEntity(
                id=current_id,
                name=f"FAKE_{first_name} FAKE_{last_name}",
                responsibleType=responsibleType,
                contactDetails=contact_details
            )
            patch.responsibleEntities = [responsible_entity]

            patch.versionNumber += 1
            table.put_item(Item=asdict(patch))
            print(f"Updated {patch.name} with {first_name} {last_name} - {contact_details.emailAddress}")

        except Exception as error:
            print(f"Skipped {patch.name} because of {error}")


def main():
    table = get_dynamodb_table("PatchesAndAreas", Stage.HOUSING_DEVELOPMENT)
    link_patches_to_areas(table)
    set_fake_responsible_entities(table)


if __name__ == "__main__":
    main()
