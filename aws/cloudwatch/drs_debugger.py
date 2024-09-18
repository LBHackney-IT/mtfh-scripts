import boto3
from boto3.session import Session
from mypy_boto3_logs import CloudWatchLogsClient
from datetime import datetime, timedelta
from enums.enums import Stage

GROUP_NAME = "/aws/lambda/contact-details-api-development"


def get_cloudwatch_logs(
    log_group_name: str,
    filter_pattern: str = "",
    start_time=datetime.today() - timedelta(days=1),
    end_time=datetime.today() + timedelta(days=1),
) -> None:
    session: Session = boto3.Session(
        profile_name=Stage.HOUSING_DEVELOPMENT.value, region_name="eu-west-2"
    )

    client: CloudWatchLogsClient = session.client("logs")

    response = client.filter_log_events(
        logGroupName=log_group_name,
        filterPattern=filter_pattern,
        startTime=int(start_time.timestamp() * 1000),
        endTime=int(end_time.timestamp() * 1000),
    )

    events = response["events"]
    for event in events:
        assert "message" in event
        print(event["message"])


if __name__ == "__main__":
    get_cloudwatch_logs(GROUP_NAME, filter_pattern="ControllerActionInvoker")
