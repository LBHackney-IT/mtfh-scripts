from dataclasses import dataclass

from aws.src.database.domain.dynamo_domain_objects import Patch


@dataclass
class StaffMemberEsPatch:
    areaId: str
    id: str
    name: str
    areaName: str

    @classmethod
    def from_patch(cls, patch: Patch, area: Patch):
        return cls(
            name=patch.name,
            id=patch.id,
            areaId=patch.parentId,
            areaName=area.name,
        )


@dataclass
class StaffMemberEs:
    patchName: str
    firstName: str
    lastName: str
    patchId: str
    areaId: str
    areaName: str
    patches: list[StaffMemberEsPatch]
    email: str

    @classmethod
    def from_patch(cls, patch: Patch, all_patches: list[Patch]):
        if patch.patchType.lower() == "patch":
            parents = [poa for poa in all_patches if poa.id.strip() == patch.parentId.strip()]
        else:
            parents = [poa for poa in all_patches if poa.name.lower().strip() == "hackney"]
        if not parents:
            raise ValueError(f"Could not find area for patch {patch.id} {patch.name}")
        parent_area = parents[0]
        return cls(
            patchName=patch.name,
            firstName=patch.responsibleEntities[0].name.split(" ")[0],
            lastName=patch.responsibleEntities[0].name.split(" ")[-1],
            patchId=patch.id,
            areaId=patch.parentId,
            areaName=parent_area.name,
            patches=[StaffMemberEsPatch.from_patch(patch, parent_area)],
            email=patch.responsibleEntities[0].contactDetails.emailAddress
        )

    @classmethod
    def from_es(cls, es_data: dict):
        source = es_data["_source"]
        return cls(
            patchName=source["patchName"],
            firstName=source["firstName"],
            lastName=source["lastName"],
            patchId=source["patchId"],
            areaId=source["areaId"],
            areaName=source["areaName"],
            patches=[StaffMemberEsPatch(**patch) for patch in source["patches"]],
            email=source["email"]
        )
