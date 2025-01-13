from aws import authentication
from enums.enums import Stage
from mypy_boto3_sqs import SQSClient
import json
from mypy_boto3_sqs.type_defs import MessageTypeDef


def get_all_dlq_messages(sqs: SQSClient, queue_url: str) -> list[MessageTypeDef]:
    messages: list[MessageTypeDef] = []
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Maximum allowed by SQS
            WaitTimeSeconds=5,  # Use long polling for efficiency
            VisibilityTimeout=30,  # Give yourself time to process
        )

        if "Messages" not in response:
            print("No more messages in the DLQ.")
            break

        for message in response["Messages"]:
            assert "Body" in message, "Message has no body"
            assert "MessageId" in message, "Message has no ID"
            print(f"Processing message ID: {message['MessageId']}")
            message["Body"] = json.loads(message["Body"])
            messages.append(message)
    return messages


def activity_history_messages_to_json():
    sqs: SQSClient = authentication.generate_aws_service(
        "sqs", stage=Stage.HOUSING_PRODUCTION
    )

    queue_name = "activityhistorydeadletterqueue.fifo"
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    bodies = get_all_dlq_messages(sqs, queue_url)

    with open("messages.json", "w") as f:
        json.dump(bodies, f)


def main():
    clean_messages = []
    with open("messages.json", "r") as f:
        messages = json.load(f)
    for message in messages:
        body = json.loads(message["Body"])
        message["Body"] = body
        clean_messages.append(message)
    with open("clean_messages.json", "w") as f:
        json.dump(clean_messages, f)


if __name__ == "__main__":
    main()
