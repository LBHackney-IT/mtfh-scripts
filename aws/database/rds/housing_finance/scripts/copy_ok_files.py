import re
from datetime import date

from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.rds.housing_finance.entities.GoogleFileSetting import (
    GoogleFileSetting,
)
from aws.database.rds.housing_finance.session_for_hfs import session_for_hfs
from enums.enums import Stage
from gcp.service_account.utils.DriveServiceAccountClient import (
    DriveServiceAccountClient,
)

FILE_LABELS = ["CashFile", "HousingFile"]
DATE_PATTERN = re.compile(r"\d{8}")


def copy_ok_files(stage: Stage):
    ssm: SSMClient = generate_aws_service("ssm", stage)
    google_api_key = ssm.get_parameter(
        Name=f"/housing-finance/{stage.to_env_name()}/google-api-key",
        WithDecryption=True,
    )["Parameter"].get("Value")
    assert google_api_key, "Google API key not found in parameter store"

    drive_client = DriveServiceAccountClient(google_api_key)

    HfsSession = session_for_hfs(stage, expire_on_commit=False)
    with HfsSession.begin() as session:
        settings = (
            session.query(GoogleFileSetting)
            .where(GoogleFileSetting.Label.in_(FILE_LABELS))
            .all()
        )

    today = date.today().strftime("%Y%m%d")

    for setting in settings:
        folder_id = setting.GoogleIdentifier
        label = setting.Label

        files = drive_client.query_files(
            [f"'{folder_id}' in parents", "trashed = false"]
        )

        ok_files = [
            f for f in files if "OK" in f["name"] and DATE_PATTERN.search(f["name"])
        ]

        if not ok_files:
            print(f"[{label}] No files with both a date and 'OK' in name — skipping.")
            continue

        def file_date(file: dict) -> str:
            match = DATE_PATTERN.search(file["name"])
            return match.group(0) if match else ""

        latest_file = max(ok_files, key=file_date)
        new_name = f"{label}{today}"

        print(
            f"[{label}] Copying '{latest_file['name']}' -> '{new_name}' in folder {folder_id}"
        )
        drive_client.copy_file(latest_file["id"], new_name, folder_id)
        print(f"[{label}] Done.")


if __name__ == "__main__":
    copy_ok_files(Stage.HOUSING_STAGING)
