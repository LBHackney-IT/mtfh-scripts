"""
Checks if tenure asset and tenure household members have 2-way referential integrity
"""
from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import Tenure, Asset, AssetTenure, Person, \
    PersonTenure
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.safe_object_from_dict import safe_object_from_dict
from enums.enums import Stage
from dateutil.parser import parse

TENURE_ID = "98bd38bd-411d-460c-b0bf-a74871f9dde3"
# TENURE_ID = "4c148ab7-a58b-a4c2-053a-03bc1684b559"  # stg


class Config:
    STAGE = Stage.HOUSING_PRODUCTION


@dataclass
class TenureMismatch:
    tenurePk: str
    tenureKey: str
    tenureValue: str
    otherPk: str
    otherType: str
    otherKey: str | None
    otherValue: str | None

    def __str__(self):
        return f"Tenure {self.tenureKey} {self.tenureValue} != {self.otherType} {self.otherKey} {self.otherValue}"


def dates_match(date_1: str, date_2: str) -> bool:
    """
    Checks if two dates are the same
    :return: True if dates are the same, False otherwise
    """
    if date_1 is None and date_2 is None:
        return True
    elif date_1 is None or date_2 is None:
        return False
    else:
        return parse(date_1).date() == parse(date_2).date()


def get_mismatches_for_asset(asset_table: Table, tenure: Tenure) -> list[TenureMismatch]:
    """
    Checks if tenure asset and asset tenure have 2-way referential integrity
    :param asset_table: Asset DynamoDB table
    :param tenure: tenure to check related asset for
    :return: list of mismatches
    """

    mismatches: list[TenureMismatch] = []
    asset_raw = asset_table.get_item(Key={"id": tenure.tenuredAsset.id})["Item"]
    asset: Asset = safe_object_from_dict(Asset, asset_raw)
    asset_tenure: AssetTenure = asset.tenure

    if asset_tenure.id != TENURE_ID:
        mismatches.append(TenureMismatch(
            tenurePk=tenure.id, tenureKey="id", tenureValue=tenure.id,
            otherPk=asset_tenure.id, otherType="assetTenure", otherKey="id", otherValue=asset_tenure.id
        ))
    if asset_tenure.type != tenure.tenureType.get('description'):
        mismatches.append(TenureMismatch(
            tenurePk=tenure.id,
            tenureKey="tenureType", tenureValue=tenure.tenureType.get('description'),
            otherPk=asset_tenure.id, otherType="assetTenure", otherKey="type", otherValue=asset_tenure.type
        ))
    if not dates_match(tenure.startOfTenureDate, asset_tenure.startOfTenureDate):
        mismatches.append(TenureMismatch(
            tenurePk=tenure.id, tenureKey="startOfTenureDate", tenureValue=tenure.startOfTenureDate,
            otherPk=asset_tenure.id, otherType="assetTenure", otherKey="startOfTenureDate", otherValue=asset_tenure.startOfTenureDate
        ))

    if not dates_match(tenure.endOfTenureDate, asset_tenure.endOfTenureDate):
        mismatches.append(TenureMismatch(
            tenurePk=tenure.id, tenureKey="endOfTenureDate", tenureValue=tenure.endOfTenureDate,
            otherPk=asset_tenure.id, otherType="assetTenure", otherKey="endOfTenureDate", otherValue=asset_tenure.endOfTenureDate
        ))
    if asset_tenure.paymentReference != tenure.paymentReference:
        mismatches.append(TenureMismatch(
            tenurePk=tenure.id, tenureKey="paymentReference", tenureValue=tenure.paymentReference,
            otherPk=asset_tenure.id, otherType="assetTenure", otherKey="paymentReference", otherValue=asset_tenure.paymentReference
        ))
    return mismatches


def get_mismatches_for_people(person_table: Table, tenure: Tenure) -> list[TenureMismatch]:
    mismatches: list[TenureMismatch] = []
    for tenure_member in tenure.householdMembers:
        person_raw = person_table.get_item(Key={"id": tenure_member.id})["Item"]
        person: Person = safe_object_from_dict(Person, person_raw)
        person_tenures: list[PersonTenure] = person.tenures

        found = False
        for person_tenure in person_tenures:
            if tenure.id == TENURE_ID:
                found = True
                if person_tenure.type != tenure.tenureType.get('description'):
                    mismatches.append(TenureMismatch(
                        tenurePk=tenure.id, otherPk=person_tenure.id,
                        tenureKey="tenureType", tenureValue=tenure.tenureType.get('description'),
                        otherType="personTenure", otherKey="type", otherValue=person_tenure.type
                    ))
                if not dates_match(tenure.startOfTenureDate, person_tenure.startDate):
                    mismatches.append(TenureMismatch(
                        tenurePk=tenure.id, otherPk=person_tenure.id,
                        tenureKey="startOfTenureDate", tenureValue=parse(tenure.startOfTenureDate).date().isoformat(),
                        otherType="personTenure", otherKey="startDate",
                        otherValue=parse(person_tenure.startDate).date().isoformat()
                    ))
                if not dates_match(tenure.endOfTenureDate, person_tenure.endDate):
                    mismatches.append(TenureMismatch(
                        tenurePk=tenure.id, otherPk=person_tenure.id,
                        tenureKey="endOfTenureDate", tenureValue=parse(tenure.endOfTenureDate).date().isoformat(),
                        otherType="personTenure", otherKey="endDate", otherValue=parse(person_tenure.endDate).date().isoformat()
                    ))
                if person_tenure.paymentReference != tenure.paymentReference:
                    mismatches.append(TenureMismatch(
                        tenurePk=tenure.id, otherPk=person_tenure.id,
                        tenureKey="paymentReference", tenureValue=tenure.paymentReference,
                        otherType="personTenure", otherKey="paymentReference",
                        otherValue=person_tenure.paymentReference
                    ))
        if not found:
            mismatches.append(TenureMismatch(
                tenurePk=tenure.id, otherPk=person_tenure.id,
                tenureKey="id", tenureValue=tenure.id, otherType="personTenure", otherKey=None, otherValue=None
            ))
    return mismatches


def main():
    mismatches: list[TenureMismatch] = []

    tenure_table = get_dynamodb_table("TenureInformation", Config.STAGE)
    tenure_raw = tenure_table.get_item(Key={"id": TENURE_ID})["Item"]
    tenure: Tenure = safe_object_from_dict(Tenure, tenure_raw)

    asset_table = get_dynamodb_table("Assets", Config.STAGE)
    mismatches += get_mismatches_for_asset(asset_table, tenure)

    person_table = get_dynamodb_table("Persons", Config.STAGE)
    mismatches += get_mismatches_for_people(person_table, tenure)

    print(f"\n\n === {len(mismatches)} mismatches found for tenure === ")
    if len(mismatches) == 0:
        print(f"No mismatches found for tenure {TENURE_ID}")
    for mismatch in mismatches:
        print(mismatch)


if __name__ == "__main__":
    main()
