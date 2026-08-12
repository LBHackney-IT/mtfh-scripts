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
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from aws.utils.csv_to_dict_list import csv_to_dict_list
import json

@dataclass
class Config:
    STAGE = Stage.HOUSING_DEVELOPMENT 
    DB_LOCAL_PORT = 6005
    THREAD_POOL_COUNT = 50
    LOG_FILE_PATH = "successfully_created_jobs.txt"
    # SOURCE_FILE_PATH = "data/jobs_to_load.tsv"
    SOURCE_FILE_PATH = "data/new.csv"
    REQUEST_BODY_FILE_PATH = "data/request_bodies.json"

@dataclass
class CSV_KEYS:
    description_key = 'Description'
    sor_code_key = "SorCode"
    unique_id_key = 'Unique Id'
    prop_ref_key = 'Property Reference'
    priority_key = "Priority"

@dataclass
class Job:
    """One CSV row as it moves through build -> review -> send."""
    unique_id: str
    # row: dict
    payload: WorkOrderPayload


session = get_session_for_stage(Config.STAGE)
asset_dynamodb_table = get_dynamodb_table("Assets", Config.STAGE)
ssm_client: SSMClient = session.client("ssm")
RepairsSession = session_for_repairs(Config.STAGE, expire_on_commit=True, local_port=Config.DB_LOCAL_PORT)

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

def get_sor_priorities(session: Session) -> dict[str, SORPriority]:
    stmt = select(SORPriority).where(SORPriority.enabled.is_(True))
    results = session.scalars(stmt).all()
    if not results:
        raise LookupError("No priorities found")
    return {priority.description: priority for priority in results}

def get_trade(session: Session, code: str) -> Trade:
    return fetch_one(session, select(Trade).where(Trade.code == code), label="trades")

def get_sor_codes(session: Session, codes: set[str]) -> dict[str, SorCode]:
    stmt = select(SorCode).where(SorCode.enabled.is_(True)).where(SorCode.code.in_(codes))
    results = session.scalars(stmt).all()
    return {sor_code.code: sor_code for sor_code in results}

def get_contractor(session: Session, reference: str) -> Contractor:
    return fetch_one(session, select(Contractor).where(Contractor.reference == reference), label="contractors")
        

def build_work_order_payload(
    row: dict,
    budget_code: BudgetCode,
    trade: Trade,
    sor_code: SorCode,
    contractor: Contractor,  

) -> Job:
    # Hardcoded values (unlikely to change)
    customer_name = "n/a"
    customer_number = "0000"
    instructed_by = "Hackney Housing" # Default hackney TMO value

    property_reference = row[CSV_KEYS.prop_ref_key]
    priority = row["priority_from_db"]
    description = row[CSV_KEYS.description_key]

    # Fetch property from asset DB
    property = get_asset_by_prop_ref(property_reference)

    # Define request body
    sorCodes : list[RateScheduleItemDict] =[{
        "customCode": sor_code.code,
        "customName": sor_code.short_description,
        "quantity": {"amount": [1]},
    }]

    payload: WorkOrderPayload = {
        "reference": [{"id": str(uuid.uuid4())}],
        "descriptionOfWork": description,
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
        "instructedBy": {"name": instructed_by},
        "assignedToPrimary": {
            "name": contractor.name,
            "organization": {"reference": [{"id": contractor.reference}]},
        },
        "customer": {
            "name": customer_name,
            "person": {
                "name": {"full": customer_name},
                "communication": [
                    {
                        "channel": {"medium": "20", "code": "60"},
                        "value": customer_number,
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

    return Job(
        unique_id=row[CSV_KEYS.unique_id_key],
        payload=payload,
        # row = row
    )


def load_completed_jobs(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}

def map_and_validate_priorities(results: list[dict], all_priorities: dict[str, SORPriority]):
    missing_priorities = {
        row[CSV_KEYS.priority_key]
        for row in results
        if PRIORITY_NAME_TO_DESCRIPTION.get(row[CSV_KEYS.priority_key]) not in all_priorities
    }
    if missing_priorities:
        raise ValueError(f"Unknown priority values, not found in database: {missing_priorities}")

    for row in results:
        row["priority_from_db"] = all_priorities[PRIORITY_NAME_TO_DESCRIPTION[row[CSV_KEYS.priority_key]]]

def validate_missing_sor_codes(results: list[dict], all_sor_codes: dict[str, SorCode]):
    missing_codes = {row[CSV_KEYS.sor_code_key] for row in results if row[CSV_KEYS.sor_code_key] not in all_sor_codes}
    if missing_codes:
        raise ValueError(f"Unknown SOR codes, not found in database: {missing_codes}")

def main():
    # Temporary hardcoded values (this should all be the same for a given bulk upload)
    trade_code = "PL"
    contractor_reference = "RG2"
    corporate_subjective_code="200045"
    external_cost_code="H2555"

    # Slice the first 5 rows
    results = csv_to_dict_list(Config.SOURCE_FILE_PATH, is_tsv=False)[:5]
    completed = load_completed_jobs(Config.LOG_FILE_PATH)

    # Filter out completed jobs
    results = [row for row in results if str(row[CSV_KEYS.unique_id_key]) not in completed]

    if not results:
        print("Nothing left to process.")
        return
    
    # Extract SOR Codes
    extracted_sor_codes = {row[CSV_KEYS.sor_code_key] for row in results}
    
    # Fetch data from RepairsDB
    with RepairsSession() as db_session:
        budget_code = get_budget_code(db_session, corporate_subjective_code, external_cost_code)
        all_priorities = get_sor_priorities(db_session)
        trade = get_trade(db_session, trade_code)
        contractor = get_contractor(db_session, contractor_reference)
        all_sor_codes = get_sor_codes(db_session, extracted_sor_codes)

    validate_missing_sor_codes(results, all_sor_codes)
    map_and_validate_priorities(results, all_priorities)


    progress_lock = Lock()
    job_list: list[Job] = []

    with progress.Bar("Generating request payloads", max=len(results)) as progress_bar:
        with ThreadPoolExecutor(max_workers=Config.THREAD_POOL_COUNT) as executor:
            futures = {
                executor.submit(build_work_order_payload, row, budget_code, trade, all_sor_codes[row[CSV_KEYS.sor_code_key]], contractor): row[CSV_KEYS.unique_id_key]
                for row in results
            }

            for future in as_completed(futures):
                unique_id = futures[future]

                try:
                    job = future.result()
                    job_list.append(job)
                except Exception as e:
                    print(f"Failed on {unique_id}: {e}")
                finally:
                    with progress_lock:
                        progress_bar.next()


    with open(Config.REQUEST_BODY_FILE_PATH, 'w') as filetowrite:
        request_bodies = [job.payload for job in job_list]
        json.dump(request_bodies, filetowrite, indent=4)

    assert input(f"You can confirm the request bodies at '{Config.REQUEST_BODY_FILE_PATH}'. Press y to continue bulk upload") == "y"

    progress_lock = Lock()

    with progress.Bar("Creating workOrders", max=len(job_list)) as progress_bar:
        failed = []

        with ThreadPoolExecutor(max_workers=Config.THREAD_POOL_COUNT) as executor:
            futures = {
                executor.submit(create_work_order_via_api, job.payload): job.unique_id
                for job in job_list
            }

            for future in as_completed(futures):
                unique_id = futures[future]

                try:
                    success = future.result()
                    if not success:
                        failed.append(unique_id)
                    else:
                        with open(Config.LOG_FILE_PATH, "a") as f:
                            f.write(f"{unique_id}\n")
                except Exception as e:
                    print(f"Failed on {unique_id}: {e}")
                    failed.append(unique_id)
                finally:
                    with progress_lock:
                        progress_bar.next()

if __name__ == "__main__":
    main()