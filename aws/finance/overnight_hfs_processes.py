"""
Trigger the overnight HFS process Lambda functions in series with real-time log streaming.
"""

import time
import logging
from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

# Configure logging to write to file and flush immediately
logging.basicConfig(
    filename="overnight_hfs_processes.txt",
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(message)s",
)

stage = Stage.DISASTER_RECOVERY
lambda_client = generate_aws_service("lambda", stage)
logs_client = generate_aws_service("logs", stage)

BASE_NAME = f"housing-finance-interim-api-{stage.to_env_name()}"
lambda_functions = [
    f"{BASE_NAME}-load-tenagree",
    f"{BASE_NAME}-cash-file",
    f"{BASE_NAME}-cash-file-trans",
    f"{BASE_NAME}-direct-debit",
    f"{BASE_NAME}-direct-debit-trans",
    f"{BASE_NAME}-adjustments-trans",
    f"{BASE_NAME}-action-diary",
    f"{BASE_NAME}-susp-cash",
    f"{BASE_NAME}-susp-hb",
    f"{BASE_NAME}-refresh-cur-bal",
    f"{BASE_NAME}-refresh-ma-cur-bal",
    f"{BASE_NAME}-refresh-op-bal",
    f"{BASE_NAME}-rent-position",
]


def stream_logs(function_name: str, start_time: int):
    """Poll CloudWatch Logs for the given function and stream them to the log file."""
    log_group = f"/aws/lambda/{function_name}"
    last_timestamp = start_time

    # We poll until we see the 'REPORT' line which indicates the end of execution
    finished = False

    while not finished:
        try:
            # Get the most recent log stream
            streams = logs_client.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            ).get("logStreams", [])

            if not streams:
                time.sleep(2)
                continue

            log_stream_name = streams[0]["logStreamName"]

            # Fetch events since the last timestamp
            events_resp = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream_name,
                startTime=last_timestamp,
                startFromHead=True,
            )

            events = events_resp.get("events", [])
            for event in events:
                msg = event["message"].strip()
                # Update timestamp to avoid duplicates in next poll
                last_timestamp = event["timestamp"] + 1

                # Write to file in real-time
                logging.info(f"[{function_name}] {msg}")

                # Check for completion markers
                if "REPORT RequestId" in msg:
                    finished = True

            # Small delay to prevent API throttling
            time.sleep(1)

        except Exception as e:
            logging.error(f"Error streaming logs for {function_name}: {str(e)}")
            break


def trigger_lambda(function_name: str) -> None:
    """Trigger the Lambda asynchronously and start the log streaming loop."""
    print(f"🚀 Triggering Lambda: {function_name}")
    logging.info(f"\n{'='*60}\nSTARTING EXECUTION: {function_name}\n{'='*60}")

    # Capture the start time in milliseconds for CloudWatch filtering
    start_ts = int(time.time() * 1000)

    # Trigger the Lambda (Event type means it returns immediately)
    response = lambda_client.invoke(FunctionName=function_name, InvocationType="Event")

    if response["StatusCode"] == 202:
        # Start the real-time log streaming loop
        stream_logs(function_name, start_ts)
        print(f"✅ {function_name} completed.")
    else:
        error_msg = (
            f"Failed to trigger {function_name}. Status: {response['StatusCode']}"
        )
        print(f"❌ {error_msg}")
        logging.error(error_msg)


def main() -> None:
    """Execute the list of functions in sequence."""
    start_all = time.time()

    # TODO: This could be improved by including running the ECS task for charges processing
    for function in lambda_functions:
        trigger_lambda(function)

    total_duration = time.time() - start_all
    summary = f"\nOvernight process finished in {total_duration/60:.2f} minutes."
    print(summary)
    logging.info(summary)


if __name__ == "__main__":
    main()
