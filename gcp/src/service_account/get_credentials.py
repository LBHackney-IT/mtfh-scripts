from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

# Set the stage to the stage you want to use
_stage = Stage.HOUSING_PRODUCTION

_ssm: SSMClient = generate_aws_service("ssm", _stage, "client")

# Modify this check if you're working with a different account
if _stage == Stage.HOUSING_DEVELOPMENT:
    _url_stage = "development"
elif _stage == Stage.HOUSING_STAGING:
    _url_stage = "staging"
elif _stage == Stage.HOUSING_PRODUCTION:
    _url_stage = "production"
else:
    raise ValueError(f"Invalid stage: {_stage}")

# Path for credentials file in parameter store
_path = f"/housing-finance/{_url_stage}/google-application-credentials-json"
CREDENTIALS = _ssm.get_parameter(Name=_path)["Parameter"]["Value"]
