from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict

from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


class EntityEventSns(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    eventType: str
    sourceDomain: str
    sourceSystem: str
    version: str
    correlationId: str
    dateTime: datetime
    user: User
    entityId: str
    eventData: EventData

    class EventData(BaseModel):
        oldData: Optional[Dict]
        newData: Dict

    class User(BaseModel):
        name: str
        email: str


def create_sns_message(
    event_type: str,
    user: EntityEventSns.User,
    entity_id: str,
    event_data: EntityEventSns.EventData,
) -> EntityEventSns:
    return EntityEventSns(
        id=entity_id,
        eventType=event_type,
        sourceDomain="mtfh-api",
        sourceSystem="mtfh-api",
        version="v1",
        correlationId=str(uuid.uuid4()),
        dateTime=datetime.now(timezone.utc),
        user=user,
        entityId=entity_id,
        eventData=event_data,
    )


def main():
    sns_client = generate_aws_service("sns", Stage.DISASTER_RECOVERY)
    event: EntityEventSns = create_sns_message(
        user=EntityEventSns.User(name="System", email="system@example.com"),
        event_type="TenureCreatedEvent",
        entity_id="de414b82-24c0-aaed-64e7-c51f79b1a08d",
        event_data=EntityEventSns.EventData(oldData={}, newData={}),
    )
    topic_arn = "arn:aws:sns:eu-west-2:851725205572:cautionaryalerts.fifo"
    print(event.model_dump_json())
    sns_client.publish(
        Message=event.model_dump_json(),
        TopicArn=topic_arn,
        MessageGroupId="fake",
    )
    print("Published event")


if __name__ == "__main__":
    main()
