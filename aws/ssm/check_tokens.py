import base64
import json
from typing import Any, cast

from mypy_boto3_ssm.client import SSMClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from enums.enums import Stage

STAGE = Stage.HOUSING_PRODUCTION


def _decode_jwt_part(part: str) -> Any:
    padding = (4 - len(part) % 4) % 4
    decoded = base64.urlsafe_b64decode(part + "=" * padding)
    return json.loads(decoded)


def try_parse_jwt(value: str) -> tuple[bool, Any, Any]:
    """Return (is_jwt, header_dict, payload_dict). All falsy if not a JWT."""
    parts = value.split(".")
    if len(parts) != 3:
        return False, None, None
    try:
        header = _decode_jwt_part(parts[0])
        payload = _decode_jwt_part(parts[1])
        return True, header, payload
    except Exception:
        return False, None, None


def get_all_parameters(ssm_client: SSMClient) -> list[dict]:
    parameters: list[dict] = []
    paginator = ssm_client.get_paginator("get_parameters_by_path")
    for page in paginator.paginate(Path="/", Recursive=True, WithDecryption=True):
        parameters.extend(page["Parameters"])
    return parameters


OUTPUT_FILE = f"ssm_parameters_{STAGE.value}.txt"


def main():
    session = get_session_for_stage(STAGE)
    ssm_client = cast(SSMClient, session.client("ssm"))

    print(f"Fetching all SSM parameters for [{STAGE.value}]...")
    parameters = get_all_parameters(ssm_client)
    print(f"Found {len(parameters)} parameters. Writing to {OUTPUT_FILE}...")

    jwt_count = 0
    with open(OUTPUT_FILE, "w") as f:
        f.write(f"SSM Parameters for [{STAGE.value}]\n")
        f.write("=" * 80 + "\n")

        for param in parameters:
            name = param["Name"]
            value = param.get("Value", "")
            is_jwt, header, payload = try_parse_jwt(value)

            if not is_jwt:
                continue

            if not "email" in payload:
                # Machine token, that's fine
                continue

            if "e2e" in payload.get("email", ""):
                # E2E test token, also fine
                continue

            payload["group_count"] = len(payload.get("groups", []))
            del payload["groups"]

            jwt_count += 1
            f.write(f"\nParameter : {name}\n")
            f.write(f"  Header  : {json.dumps(header)}\n")
            f.write(f"  Payload : {json.dumps(payload, indent=2)}\n")
            f.write("-" * 80 + "\n")

        f.write(f"\n{'=' * 80}\n")
        f.write(f"Done. {jwt_count} of {len(parameters)} parameters were JWTs.\n")

    print(f"Done. Results written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
