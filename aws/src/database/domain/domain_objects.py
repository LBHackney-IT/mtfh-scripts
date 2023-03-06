from dataclasses import dataclass
from uuid import UUID


@dataclass
class Tenure:
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
class Person:
    id: str
    title: str
    firstName: str
    middleName: str | None
    surname: str
    dateOfBirth: str | None
    personTypes: list[str]
    tenures: list[Tenure]


@dataclass
class ResponsibleEntity:
    id: id
    name: str
    responsibleType: str


@dataclass
class Tenure:
    id: str
    startOfTenureDate: str
    paymentReference: int
    type: str
    endOfTenureDate: str = None


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
    tenure: dict
    versionNumber: int
