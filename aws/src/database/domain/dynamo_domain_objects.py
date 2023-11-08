from typing import TypeVar, Type

from _decimal import Decimal
from dataclasses import dataclass

from aws.src.utils.safe_object_from_dict import safe_object_from_dict

T = TypeVar("T")


def _dataclass_from_data(cls: Type[T], data: dict | T | None) -> T:
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
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


@dataclass
class HouseholdMember:
    id: str
    fullName: str | None
    dateOfBirth: str | None
    isResponsible: bool | None
    personTenureType: str | None
    type: str | None

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)

@dataclass
class Tenure:
    id: str
    charges: dict | None
    endOfTenureDate: str | None
    evictionDate: str | None
    householdMembers: list[HouseholdMember]
    informHousingBenefitsForChanges: bool | None
    isMutalExchange: bool | None
    isSublet: bool | None
    legacyReferences: list[dict]
    notices: list[dict]
    paymentReference: str | None
    potentialEndDate: str | None
    startOfTenureDate: str | None
    subletEndDate: str | None
    successionDate: str | None
    tenuredAsset: TenuredAsset | None
    tenureType: dict | None
    terminated: dict | None

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)

    def __post_init__(self):
        if self.householdMembers is None:
            self.householdMembers = []

        if self.legacyReferences is None:
            self.legacyReferences = []

        if self.notices is None:
            self.notices = []

        self.tenuredAsset = TenuredAsset.from_data(self.tenuredAsset)
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
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


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
        if self.personTypes is None:
            self.personTypes = []
        self.tenures = [PersonTenure.from_data(tenure) for tenure in self.tenures] if self.tenures is not None else []

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


# --- END Person Table ---


# --- Asset Table ---
@dataclass
class ResponsibleEntityContactDetails:
    emailAddress: str | None

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


@dataclass
class ResponsibleEntity:
    id: str
    name: str | None
    responsibleType: str | None
    contactDetails: ResponsibleEntityContactDetails | None

    def __post_init__(self):
        self.contactDetails = ResponsibleEntityContactDetails.from_data(self.contactDetails)

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


@dataclass
class Patch:
    id: str
    domain: str | None
    name: str | None
    parentId: str | None
    patchType: str | None
    responsibleEntities: list[ResponsibleEntity]
    versionNumber: int | None

    def __post_init__(self):
        if self.responsibleEntities is None:
            self.responsibleEntities = []
        self.responsibleEntities = [ResponsibleEntity.from_data(entity) for entity in self.responsibleEntities]

    @classmethod
    def from_data(cls, data):
        return _dataclass_from_data(cls, data)


@dataclass
class AssetTenure:
    id: str | None
    startOfTenureDate: str | None
    paymentReference: str | None
    type: str | None
    endOfTenureDate: str | None

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)
    

@dataclass
class AssetAddress:
    addressLine1: str | None
    addressLine2: str | None
    addressLine3: str | None
    addressLine4: str | None
    postCode: str | None
    postPreamble: str | None
    uprn: str | None

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)


@dataclass
class Asset:
    id: str
    assetAddress: AssetAddress | None
    assetCharacteristics: dict | None
    assetId: str | None
    assetLocation: dict | None
    assetManagement: dict | None
    assetType: str | None
    isActive: Decimal | None
    patches: list[Patch]
    rootAsset: str | None
    tenure: AssetTenure | None
    parentAssetIds: str | None
    versionNumber: Decimal | None

    def __post_init__(self):
        if self.patches is None:
            self.patches = []

        self.tenure = AssetTenure.from_data(self.tenure)
        self.assetAddress = AssetAddress.from_data(self.assetAddress)
        if not all(isinstance(patch, Patch) for patch in self.patches):
            self.patches = [Patch.from_data(patch) for patch in self.patches]

    @classmethod
    def from_data(cls, data: dict):
        return _dataclass_from_data(cls, data)

# --- END Asset Table ---
