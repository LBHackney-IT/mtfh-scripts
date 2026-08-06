from typing import TypedDict
from enum import Enum

class ReferenceDict(TypedDict):
    id: str


class PriorityDict(TypedDict):
    priorityCode: int
    priorityDescription: str
    numberOfDays: int


class WorkClassDict(TypedDict):
    workClassCode: int


class QuantityDict(TypedDict):
    amount: list[float]


class RateScheduleItemDict(TypedDict):
    customCode: str
    customName: str
    quantity: QuantityDict


class TradeDict(TypedDict):
    code: str
    customCode: str
    customName: str


class WorkElementDict(TypedDict):
    rateScheduleItem: list[RateScheduleItemDict]
    trade: list[TradeDict]


class AddressDict(TypedDict):
    addressLine: list[str]
    postalCode: str


class PropertyDict(TypedDict):
    propertyReference: str
    address: AddressDict
    reference: list[ReferenceDict]


class SiteDict(TypedDict):
    property: list[PropertyDict]


class InstructedByDict(TypedDict):
    name: str


class OrganizationDict(TypedDict):
    reference: list[ReferenceDict]


class AssignedToPrimaryDict(TypedDict):
    name: str
    organization: OrganizationDict


class ChannelDict(TypedDict):
    medium: str
    code: str


class CommunicationDict(TypedDict):
    channel: ChannelDict
    value: str


class PersonNameDict(TypedDict):
    full: str


class PersonDict(TypedDict):
    name: PersonNameDict
    communication: list[CommunicationDict]


class CustomerDict(TypedDict):
    name: str
    person: PersonDict

class BudgetCodeDict(TypedDict):
    id: int

class WorkOrderPayload(TypedDict):
    reference: list[ReferenceDict]
    descriptionOfWork: str
    priority: PriorityDict
    workClass: WorkClassDict
    workElement: list[WorkElementDict]
    site: SiteDict
    instructedBy: InstructedByDict
    assignedToPrimary: AssignedToPrimaryDict
    customer: CustomerDict
    multiTradeWorkOrder: bool
    isAwaabsDampAndMouldRepair: bool
    budgetCode: BudgetCodeDict

class Priority(str, Enum):
    IMMEDIATE = "[I] IMMEDIATE"
    EMERGENCY = "[E] EMERGENCY"
    URGENT = "[U] URGENT"
    NORMAL = "[N] NORMAL"
    PLANNED_MAINTENANCE = "[P] PLANNED MAINT"
    VOIDS_MINOR = "[V15] Voids minor"
    VOIDS_MAJOR = "[V30] Voids major"
    LEGAL_DISREPAIR = "[L] LEGAL DISREP"
    LEGAL_DISREPAIR_EPA_20_DAYS = "[L2] LD EPA 20 DAYS"
    MINOR_ADAPTATION = "[AD20] Minor Adaptation"