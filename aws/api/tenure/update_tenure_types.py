"""
Update the tenure type of tenures listed in `tenure_type_update.tsv` via the
Tenure API. The target type is taken from the row's `Tenure` column.

For each row in the TSV:
1. Look up the tenure ID from the Search API using the row's Payment Ref.
2. GET the tenure from the Tenure API to read its current version (ETag).
3. PATCH the tenure type with `If-Match: "<versionNumber>"`.

Requires:
- AWS credentials for the configured stage (used to read SSM parameters)
- HACKNEY_JWT environment variable containing a token with permission to
  edit tenure details

Results, including dry runs, are written to logs/update_tenure_types.txt.
"""

import logging
import os

import progress.bar as progress
import requests
from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.utils.csv_to_dict_list import csv_to_dict_list
from enums.enums import Stage

STAGE = Stage.HOUSING_PRODUCTION
TSV_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "tenure_type_update.tsv"
)
PAYMENT_REF_COLUMN = "Payment Ref"
TENURE_TYPE_COLUMN = "Tenure"

# Keyed by the value in the TSV's "Tenure" column, populated from
# Hackney.Shared.Tenure.Domain.TenureTypes
TENURE_TYPE_LOOKUP = {
    "Council Homes Acquisition Programme": {
        "code": "CHP",
        "description": "Council Homes Acquisition Programme",
    },
    "Hackney Housing Company": {
        "code": "HHC",
        "description": "Hackney Housing Company",
    },
    "Home Office Non-Secure": {
        "code": "HON",
        "description": "Home Office Non-Secure",
    },
    "Local Authority Housing Fund": {
        "code": "LHF",
        "description": "Local Authority Housing Fund",
    },
}

LOG_FILE = "logs/update_tenure_types.txt"
HACKNEY_JWT = os.environ["HACKNEY_JWT"]
logger = logging.getLogger(__name__)


def configure_logging() -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)


def fetch_ssm_parameter(ssm_client: SSMClient, parameter_name: str) -> str:
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    value = response["Parameter"].get("Value")
    if not value:
        raise ValueError(f"SSM parameter {parameter_name} has no value")
    return value


def get_api_url(ssm_client: SSMClient, api_name: str) -> str:
    parameter_name = f"/housing-tl/{STAGE.to_env_name()}/{api_name}-api-url"
    return fetch_ssm_parameter(ssm_client, parameter_name).rstrip("/")


def get_tenure_id_from_payment_ref(search_api_url: str, payment_ref: str) -> str:
    """Find the single tenure ID matching a payment reference"""
    response = requests.get(
        f"{search_api_url}/search/tenures",
        params={"searchText": payment_ref},
        headers={"Authorization": HACKNEY_JWT},
        timeout=30,
    )
    response.raise_for_status()

    tenures = response.json().get("results", {}).get("tenures", [])
    matches = [
        tenure for tenure in tenures if tenure.get("paymentReference") == payment_ref
    ]

    if not matches:
        raise ValueError(f"No tenure found for payment ref {payment_ref}")
    if len(matches) > 1:
        raise ValueError(
            f"Multiple tenures found for payment ref {payment_ref}: "
            f"{[tenure.get('id') for tenure in matches]}"
        )

    return matches[0]["id"]


def get_version_from_etag(response: requests.Response) -> str:
    etag = response.headers.get("ETag")
    if not etag:
        raise ValueError("GET tenure response did not include an ETag header")

    version = etag.strip()
    if version.startswith("W/"):
        version = version[2:]
    return version.strip('"')


def patch_tenure_type(
    tenure_api_url: str,
    tenure_id: str,
    version: str,
    tenure_type: dict[str, str],
) -> None:
    headers = {
        "Authorization": HACKNEY_JWT,
        "If-Match": f'"{version}"',
        "Content-Type": "application/json",
    }
    response = requests.patch(
        f"{tenure_api_url}/tenures/{tenure_id}",
        json={"tenureType": tenure_type},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()


def update_tenure_types(dry_run: bool) -> None:
    configure_logging()
    logger.info("Log file: %s", LOG_FILE)

    session = get_session_for_stage(STAGE)
    ssm_client: SSMClient = session.client("ssm")
    tenure_api_url = get_api_url(ssm_client, "tenure")
    search_api_url = get_api_url(ssm_client, "search")

    rows = csv_to_dict_list(TSV_PATH, is_tsv=True)
    rows = [row for row in rows if row.get(PAYMENT_REF_COLUMN)]

    logger.info("Updating tenure types for %s rows (dry_run=%s)", len(rows), dry_run)
    logger.info("Known tenure types: %s", list(TENURE_TYPE_LOOKUP))
    if dry_run:
        logger.info("Dry run - no changes will be made")

    patched = 0
    skipped = 0
    failures: list[tuple[str, str]] = []

    with progress.Bar("Updating tenure types", max=len(rows)) as progress_bar:
        for row in rows:
            payment_ref = str(row[PAYMENT_REF_COLUMN]).strip()
            tenure_description = str(row.get(TENURE_TYPE_COLUMN, "")).strip()
            try:
                tenure_type = TENURE_TYPE_LOOKUP.get(tenure_description)
                if tenure_type is None:
                    raise ValueError(f"Unknown tenure type '{tenure_description}'")

                tenure_id = get_tenure_id_from_payment_ref(search_api_url, payment_ref)

                get_response = requests.get(
                    f"{tenure_api_url}/tenures/{tenure_id}",
                    headers={"Authorization": HACKNEY_JWT},
                    timeout=30,
                )
                get_response.raise_for_status()
                tenure = get_response.json()
                version = get_version_from_etag(get_response)

                current_tenure_type = tenure.get("tenureType") or {}
                current_code = current_tenure_type.get("code") or "UNKNOWN"
                current_description = (
                    current_tenure_type.get("description") or "UNKNOWN"
                )
                current = f"{current_code} ({current_description})"
                target = f"{tenure_type['code']} ({tenure_type['description']})"

                type_matches = (
                    current_code == tenure_type["code"]
                    and current_description == tenure_type["description"]
                )
                if type_matches:
                    skipped += 1
                    logger.info(
                        "SKIPPED %s (%s): already %s",
                        payment_ref,
                        tenure_id,
                        current,
                    )
                elif dry_run:
                    patched += 1
                    logger.info(
                        "DRY RUN %s (%s): %s -> %s, version %s",
                        payment_ref,
                        tenure_id,
                        current,
                        target,
                        version,
                    )
                else:
                    patch_tenure_type(tenure_api_url, tenure_id, version, tenure_type)
                    patched += 1
                    logger.info(
                        "PATCHED %s (%s): %s -> %s, version %s",
                        payment_ref,
                        tenure_id,
                        current,
                        target,
                        version,
                    )
            except (requests.HTTPError, ValueError, KeyError) as error:
                response = getattr(error, "response", None)
                detail = (
                    f"{error} - {response.text}" if response is not None else str(error)
                )
                failures.append((payment_ref, detail))
                # logger.error("FAILED %s: %s", payment_ref, detail)
            finally:
                progress_bar.next()

    action = "Would patch" if dry_run else "Patched"
    logger.info("%s: %s", action, patched)
    logger.info("Skipped (already target type): %s", skipped)
    logger.info("Failed: %s", len(failures))
    for payment_ref, error in failures:
        logger.info("  %s: %s", payment_ref, error)
    logger.info("Finished")
    print(
        f"{action}: {patched}, skipped: {skipped}, failed: {len(failures)} "
        f"(log: {LOG_FILE})"
    )


def main(dry_run: bool = True) -> None:
    update_tenure_types(dry_run=dry_run)


if __name__ == "__main__":
    main(dry_run=False)
