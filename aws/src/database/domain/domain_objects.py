from dataclasses import dataclass
from uuid import UUID

@dataclass
class AssetTenure:
    id: str
    startOfTenureDate: str
    paymentReference: int
    type: str
    endOfTenureDate: str = None


@dataclass
class PersonTenure:
    id: str
    assetId: str
    propertyReference: str | None
    uprn: str | None
    assetFullAddress: str
    paymentReference: str | None
    startDate: str | None
    endDate: str | None
    type: str | None

@dataclass
class Tenure:
    id: str
    charges: list[dict]
    endOfTenureDate: str | None
    evictionDate: str | None
    householdMembers: list[dict]
    informHousingBenefitsForChanges: bool
    isMutalExchange: bool
    isSublet: bool
    legacyReferences: list[dict]
    notices: list[dict]
    paymentReference: str | None
    potentialEndDate: str | None
    startOfTenureDate: str | None
    subletEndDate: str | None
    successionDate: str | None
    tenuredAsset: dict
    tenureType: dict
    terminated: dict



@dataclass
class Person:
    id: str
    title: str
    firstName: str
    middleName: str | None
    surname: str
    dateOfBirth: str | None
    personTypes: list[str]
    tenures: list[PersonTenure]


@dataclass
class ResponsibleEntity:
    id: id
    name: str
    responsibleType: str


@dataclass
class Patch:
    id: str
    domain: str
    name: str
    parentId: str
    patchType: str
    responsibleEntities: list[ResponsibleEntity]
    versionNumber: int


@dataclass
class Asset:
    id: str
    assetAddress: dict
    assetCharacteristics: dict
    assetId: str
    assetLocation: dict
    assetManagement: dict
    assetType: str
    isActive: bool
    patches: list[Patch]
    rootAsset: str
    tenure: AssetTenure
    versionNumber: int
