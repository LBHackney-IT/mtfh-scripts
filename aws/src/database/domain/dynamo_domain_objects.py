from _decimal import Decimal
from dataclasses import dataclass


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
class Tenure:
    id: str
    charges: list[dict] | None
    endOfTenureDate: str | None
    evictionDate: str | None
    householdMembers: list[dict]
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
    tenuredAsset: dict | None
    tenureType: dict | None
    terminated: dict | None


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
    numberOfBedSpaces: str | None
