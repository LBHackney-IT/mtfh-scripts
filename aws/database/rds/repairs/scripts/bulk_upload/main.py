import os
import uuid
import requests
from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from enums.enums import Stage
from aws.authentication.generate_aws_resource import get_session_for_stage
from mypy_boto3_ssm import SSMClient
from aws.database.rds.repairs.session_for_repairs import session_for_repairs
from aws.database.rds.repairs.entities.BudgetCodeStore import BudgetCode
from aws.database.rds.repairs.entities.SORPriorityStore import SORPriority
from aws.database.rds.repairs.entities.TradeStore import Trade
from aws.database.rds.repairs.entities.SORCodeStore import SorCode
from aws.database.rds.repairs.entities.ContractorStore import Contractor
from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm import Session
from typing import TypeVar
from sqlalchemy import Select, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from aws.database.rds.repairs.scripts.bulk_upload.types import *
from dataclasses import dataclass
import progress.bar as progress

@dataclass
class Config:
    STAGE = Stage.HOUSING_DEVELOPMENT 
    DB_LOCAL_PORT = 6005

session = get_session_for_stage(Config.STAGE)
asset_dynamodb_table = get_dynamodb_table("Assets", Config.STAGE)
ssm_client: SSMClient = session.client("ssm")

# Hackney JWT for authenticating to the API
hackney_jwt = os.environ.get("HACKNEY_JWT_WORK_ORDER")
assert hackney_jwt, "HACKNEY_JWT_WORK_ORDER environment variable not set"

path_repairs_api_url = f"/repairs-hub/{Config.STAGE.to_env_name()}/repairs-service-api-url"
repairs_api_url = ssm_client.get_parameter(Name=path_repairs_api_url)["Parameter"].get("Value")
assert repairs_api_url, "repairs-service-api-url variable not set"

path_repairs_api_key = f"/repairs-hub/{Config.STAGE.to_env_name()}/repairs-service-api-key"
repairs_api_key = ssm_client.get_parameter(Name=path_repairs_api_key)["Parameter"].get("Value")
assert repairs_api_key, "repairs-service-api-key variable not set"

http = requests.Session() 
http.headers.update({"Authorization": hackney_jwt, "x-hackney-user": hackney_jwt, "x-api-key": repairs_api_key})


def create_work_order_via_api(request_body: WorkOrderPayload) -> bool:
    """POST a work order to the Work Order API."""

    response = http.post(f"{repairs_api_url}/workOrders/schedule", json=request_body, timeout=30)

    try:
        response.raise_for_status()
        return True
    except requests.HTTPError:
        print(f"Error response prop_ref:{request_body['site']['property'][0]['propertyReference']} body: {response.text}")
        return False

def get_asset_by_prop_ref(property_reference: str):
    return get_by_secondary_index(asset_dynamodb_table, "AssetId", "assetId", property_reference)

T = TypeVar("T")

def fetch_one(session: Session, stmt: Select[tuple[T]], label: str) -> T:
    try:
        return session.scalars(stmt).one()
    except NoResultFound:
        raise LookupError(f"No {label} found") from None
    except MultipleResultsFound:
        raise LookupError(f"Multiple {label} matched — expected exactly one") from None

def get_budget_code(session: Session, corporate_subjective_code: str, external_cost_code: str):
    return fetch_one(
        session, 
        select(BudgetCode)
            .where(BudgetCode.corporate_subjective_code == corporate_subjective_code)
            .where(BudgetCode.external_cost_code == external_cost_code) 
            .where(BudgetCode.cost_code.is_(None)),
        label="budget codes"
    )

def get_sor_priority(session: Session, priority: Priority) -> SORPriority:
    return fetch_one(
        session, 
        select(SORPriority).where(SORPriority.enabled.is_(True)).where(SORPriority.description == priority.value), 
        label="priorities"
    )

def get_trade(session: Session, code: str) -> Trade:
    return fetch_one(session, select(Trade).where(Trade.code == code), label="trades")

def get_sor_code(session: Session, code: str) -> SorCode:
    return fetch_one(session, select(SorCode).where(SorCode.code == code), label="sor codes")     

def get_contractor(session: Session, reference: str) -> Contractor:
    return fetch_one(session, select(Contractor).where(Contractor.reference == reference), label="contractors")
        
def main():
    # Fetch data from RepairsDB
    RepairsSession = session_for_repairs(Config.STAGE, expire_on_commit=True, local_port=Config.DB_LOCAL_PORT)

    with RepairsSession() as db_session:
        budget_code = get_budget_code(db_session, corporate_subjective_code="200045", external_cost_code="H2555")
        priority = get_sor_priority(db_session, Priority.NORMAL)
        trade = get_trade(db_session, "PL")
        sor_code = get_sor_code(db_session, "EICR0005")
        contractor = get_contractor(db_session, "RG2")

    property_list = [
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
        "00023402",
    ]
    
    with progress.Bar("Creating workOrders", max=len(property_list)) as progress_bar:

        for property_reference in property_list:
            # Fetch property from asset DB
            property = get_asset_by_prop_ref(property_reference)

            # Define request body
            sorCodes : list[RateScheduleItemDict] =[{
                "customCode": sor_code.code,
                "customName": sor_code.short_description,
                "quantity": {"amount": [1]},
            }]

            request_body: WorkOrderPayload = {
                "reference": [{"id": str(uuid.uuid4())}],
                "descriptionOfWork": "Carry out EICR including Smoke Alarms and remedials works as per agreed basket rate and upload to SAFe",
                "priority": {
                    "priorityCode": priority.priority_code, 
                    "priorityDescription": priority.description,
                    "numberOfDays": int(priority.days_to_complete)  # type: ignore[arg-type]
                },
                "workClass": {"workClassCode": 0},
                "workElement": [
                    {
                        "rateScheduleItem": [item], 
                        "trade": [{
                            "code": "SP", 
                            "customCode": trade.code, 
                            "customName": trade.name
                        }]
                    }
                    for item in sorCodes
                ],
                "site": {
                    "property": [{
                        "propertyReference": property_reference,
                        "address": {
                            "addressLine": [property[0]['assetAddress']['addressLine1']],
                            "postalCode": property[0]['assetAddress']['postCode'],
                        },
                        "reference": [{"id": property_reference}],
                    }]
                },
                "instructedBy": {"name": "Hackney Housing"},
                "assignedToPrimary": {
                    "name": contractor.name,
                    "organization": {"reference": [{"id": contractor.reference}]},
                },
                "customer": {
                    "name": "n/a",
                    "person": {
                        "name": {"full": "n/a"},
                        "communication": [
                            {
                                "channel": {"medium": "20", "code": "60"},
                                "value": "0000",
                            }
                        ],
                    },
                },
                "budgetCode": {
                    "id": budget_code.id
                },
                "multiTradeWorkOrder": False,
                "isAwaabsDampAndMouldRepair": False,
            }

            create_work_order_via_api(request_body)
            progress_bar.next()

if __name__ == "__main__":
    main()