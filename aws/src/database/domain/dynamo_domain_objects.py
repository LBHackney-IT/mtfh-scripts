from _decimal import Decimal
from dataclasses import dataclass

from aws.src.utils.safe_object_from_dict import safe_object_from_dict

# --- Tenure Table ---
@dataclass
class TenuredAsset:
    id: str
    fullAddress: str | None
    propertyReference: str | None
    uprn: str | None
    type: str | None


@dataclass
class HouseholdMember:
    id: str
    fullName: str | None
    dateOfBirth: str | None
    isResponsible: bool | None
    personTenureType: str | None
    type: str | None


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

    def __post_init__(self):
        self.tenuredAsset = safe_object_from_dict(TenuredAsset, self.tenuredAsset)
        self.householdMembers = [safe_object_from_dict(HouseholdMember, member) for member in self.householdMembers]
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
        self.tenures = [safe_object_from_dict(PersonTenure, tenure) for tenure in self.tenures]
# --- END Person Table ---


# --- Asset Table ---
@dataclass
class ResponsibleEntity:
    id: str
    name: str | None
    responsibleType: str | None


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
        self.responsibleEntities = [safe_object_from_dict(ResponsibleEntity, entity) for entity in
                                    self.responsibleEntities]


@dataclass
class AssetTenure:
    id: str | None
    startOfTenureDate: str | None
    paymentReference: str | None
    type: str | None
    endOfTenureDate: str | None


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
        self.tenure = safe_object_from_dict(AssetTenure, self.tenure)
        self.patches = [safe_object_from_dict(Patch, patch) for patch in self.patches]
# --- END Asset Table ---