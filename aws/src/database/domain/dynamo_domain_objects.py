from typing import cast, Any

from _decimal import Decimal
from dataclasses import dataclass

from aws.src.utils.safe_object_from_dict import safe_object_from_dict


def _from_data(cls, data: Any) -> object:
    """
    Converts a dict to a dataclass, or returns the dataclass if it is already one
    :param cls: The dataclass to convert to
    :param data: The data to convert
    :return: The converted dataclass
    """
    if isinstance(data, dict):
        return safe_object_from_dict(cls, data)
    elif isinstance(data, cls):
        return data
    elif data is None:
        return None
    else:
        raise ValueError(f"Cannot convert {data} to {cls}")


# --- Tenure Table ---
@dataclass
class TenuredAsset:
    id: str
    fullAddress: str | None
    propertyReference: str | None
    uprn: str | None
    type: str | None

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), TenuredAsset | None)

@dataclass
class HouseholdMember:
    id: str
    fullName: str | None
    dateOfBirth: str | None
    isResponsible: bool | None
    personTenureType: str | None
    type: str | None

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), HouseholdMember | None)


@dataclass
class Tenure:
    id: str
    charges: dict | None
    endOfTenureDate: str | None
    evictionDate: str | None
    householdMembers: list[HouseholdMember] | None
    informHousingBenefitsForChanges: bool | None
    isMutalExchange: bool | None
    isSublet: bool | None
    legacyReferences: list[dict] | None
    notices: list[dict] | None
    paymentReference: str | None
    potentialEndDate: str | None
    startOfTenureDate: str | None
    subletEndDate: str | None
    successionDate: str | None
    tenuredAsset: TenuredAsset | None
    tenureType: dict | None
    terminated: dict | None

    @classmethod
    def from_data(cls, data: Any):
        return _from_data(cls, data)

    def __post_init__(self):
        self.tenuredAsset = TenuredAsset.from_data(self.tenuredAsset)
        if not all(isinstance(member, HouseholdMember) for member in self.householdMembers):
            self.householdMembers = [HouseholdMember.from_data(member) for member in self.householdMembers]

# --- END Tenure Table ---


# --- Person Table ---
@dataclass
class PersonTenure:
    id: str
    assetId: str | None
    propertyReference: str | None
    uprn: str | None
    assetFullAddress: str | None
    paymentReference: str | None
    startDate: str | None
    endDate: str | None
    type: str | None

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), PersonTenure)


@dataclass
class Person:
    id: str
    title: str | None
    firstName: str | None
    middleName: str | None
    surname: str | None
    dateOfBirth: str | None
    personTypes: list[str]
    tenures: list[PersonTenure]

    def __post_init__(self):
        self.tenures = [PersonTenure.from_data(tenure) for tenure in self.tenures]

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), Person)

# --- END Person Table ---


# --- Asset Table ---
@dataclass
class ResponsibleEntity:
    id: str
    name: str | None
    responsibleType: str | None

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), ResponsibleEntity)


@dataclass
class Patch:
    id: str
    domain: str | None
    name: str | None
    parentId: str | None
    patchType: str | None
    responsibleEntities: list[ResponsibleEntity]
    versionNumber: Decimal | None

    def __post_init__(self):
        self.responsibleEntities = [ResponsibleEntity.from_data(entity) for entity in self.responsibleEntities]

    def from_data(self):
        return cast(_from_data(self, self), Patch)


@dataclass
class AssetTenure:
    id: str | None
    startOfTenureDate: str | None
    paymentReference: str | None
    type: str | None
    endOfTenureDate: str | None

    @classmethod
    def from_data(cls, data: Any):
        return cast(_from_data(cls, data), AssetTenure)


@dataclass
class Asset:
    id: str
    assetAddress: dict | None
    assetCharacteristics: dict | None
    assetId: str | None
    assetLocation: dict | None
    assetManagement: dict | None
    assetType: str | None
    isActive: Decimal | None
    patches: list[Patch]
    rootAsset: str | None
    tenure: AssetTenure | None
    versionNumber: Decimal | None

    def __post_init__(self):
        self.tenure = AssetTenure.from_data(self.tenure)
        self.patches = [Patch.from_data(patch) for patch in self.patches]

# --- END Asset Table ---
