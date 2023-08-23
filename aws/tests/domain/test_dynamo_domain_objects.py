import random
import uuid

import pytest

from aws.src.database.domain.dynamo_domain_objects import Tenure, HouseholdMember, TenuredAsset, Asset, AssetTenure, \
    Patch, Person, PersonTenure


def test_generates_tenure(tenure_dict: dict):
    tenure = Tenure.from_data(tenure_dict)

    assert isinstance(tenure, Tenure)
    assert tenure.id == str(uuid.uuid4())

    assert isinstance(tenure.tenuredAsset, TenuredAsset)
    assert tenure.tenuredAsset.id == str(uuid.uuid4())

    assert isinstance(tenure.householdMembers[0], HouseholdMember)
    assert tenure.householdMembers[0].fullName == 'FAKE_First FAKE_Last'


def test_generates_asset(asset_dict: dict):
    asset = Asset.from_data(asset_dict)

    assert isinstance(asset, Asset)
    assert asset.id == asset_dict.get('id')
    assert asset.assetAddress.get('addressLine1') == asset_dict.get('assetAddress').get('addressLine1')

    assert isinstance(asset.tenure, AssetTenure)
    assert asset.tenure.id == asset_dict.get('tenure').get('id')

    assert isinstance(asset.patches[0], Patch)
    assert asset.patches[0].id == asset_dict.get('patches')[0].get('id')


def test_generates_person(person_dict: dict):
    person = Person.from_data(person_dict)

    assert isinstance(person, Person)
    assert person.id == person_dict.get('id')

    assert isinstance(person.tenures[0], PersonTenure)
    assert person.tenures[0].id == person_dict.get('tenures')[0].get('id')


@pytest.fixture
def tenure_dict():
    return {
        "id": str(uuid.uuid4()),
        "charges": {
            "billingFrequency": "Weekly",
            "combinedRentCharges": 0,
            "combinedServiceCharges": 0,
            "currentBalance": 3019.14,
            "originalRentCharge": 0,
            "originalServiceCharge": 0,
            "otherCharges": 0,
            "rent": 0,
            "serviceCharge": 0,
            "tenancyInsuranceCharge": 0
        },
        "endOfTenureDate": "2017-11-06",
        "evictionDate": "1900-01-01",
        "householdMembers": [
            {
                "id": str(uuid.uuid4()),
                "dateOfBirth": "1066-07-29",
                "fullName": "FAKE_First FAKE_Last",
                "isResponsible": True,
                "personTenureType": "Tenant",
                "type": "person"
            }
        ],
        "informHousingBenefitsForChanges": False,
        "isMutualExchange": False,
        "isSublet": False,
        "legacyReferences": [
            {
                "name": "uh_tag_ref",
                "value": f"{random.randint(10 ** 7, 10 ** 8 - 1)}/01"
            },
            {
                "name": "u_saff_tenancy",
                "value": ""
            }
        ],
        "notices": [
            {
                "effectiveDate": "1900-01-01",
                "endDate": None,
                "expiryDate": "1900-01-01",
                "servedDate": "1900-01-01",
                "type": ""
            }
        ],
        "paymentReference": str(random.randint(10 ** 10, 10 ** 11 - 1)),
        "potentialEndDate": "1900-01-01",
        "startOfTenureDate": "2017-05-30",
        "subletEndDate": "1900-01-01",
        "successionDate": "1900-01-01",
        "tenuredAsset": {
            "id": str(uuid.uuid4()),
            "fullAddress": "THE HACKNEY SERVICE CENTRE 1 Hackney Service Centre E8 1DY",
            "propertyReference": str(random.randint(10 ** 7, 10 ** 8 - 1)),
            "type": "Dwelling",
            "uprn": str(random.randint(10 ** 12, 10 ** 13 - 1))
        },
        "tenureType": {
            "code": "THO",
            "description": "Temp Hostel"
        },
        "terminated": {
            "isTerminated": True,
            "reasonForTermination": ""
        }
    }


@pytest.fixture
def asset_dict():
    return {
        "id": str(uuid.uuid4()),
        "assetAddress": {
            "addressLine1": "FLAT 10 220 TEST ROAD",
            "addressLine2": "HACKNEY",
            "addressLine3": "LONDON",
            "postCode": "E8 1AA",
            "uprn": str(random.randint(10 ** 12, 10 ** 13 - 1))
        },
        "assetCharacteristics": {
            "numberOfBedrooms": 1,
            "numberOfLifts": 0,
            "numberOfLivingRooms": 0,
            "yearConstructed": "0"
        },
        "assetId": str(random.randint(10 ** 12, 10 ** 13 - 1)),
        "assetLocation": {
            "parentAssets": [
                {
                    "id": str(uuid.uuid4()),
                    "name": "Hackney Homes",
                    "type": "NA"
                }
            ],
            "totalBlockFloors": 0
        },
        "assetManagement": {
            "isCouncilProperty": False,
            "isNoRepairsMaintenance": False,
            "isTMOManaged": False,
            "managingOrganisation": "London Borough of Hackney",
            "managingOrganisationId": str(uuid.uuid4()),
            "owner": "KUS",
            "propertyOccupiedStatus": "VR"
        },
        "assetType": "Dwelling",
        "isActive": 0,
        "parentAssetIds": str(uuid.uuid4()),
        "patches": [
            {
                "id": str(uuid.uuid4()),
                "domain": "MMH",
                "name": "SN4",
                "parentId": str(uuid.uuid4()),
                "patchType": "patch",
                "responsibleEntities": [
                    {
                        "id": str(uuid.uuid4()),
                        "name": "Fake_First Fake_Last",
                        "responsibleType": "HousingOfficer"
                    }
                ],
                "versionNumber": None
            }
        ],
        "rootAsset": "ROOT",
        "tenure": {
            "id": str(uuid.uuid4()),
            "endOfTenureDate": "2050-12-12T00:00:00Z",
            "paymentReference": str(random.randint(10 ** 12, 10 ** 13 - 1)),
            "startOfTenureDate": "2030-12-12T00:00:00Z",
            "type": "Secure"
        },
        "versionNumber": 3
    }


@pytest.fixture
def person_dict():
    return {
        "id": str(uuid.uuid4()),
        "dateOfBirth": "1962-04-18T00:00:00.0000000Z",
        "firstName": "FAKE_First",
        "lastModified": "2022-09-06T06:31:03.5321566Z",
        "links": [
        ],
        "personTypes": [
            "Tenant",
            "HouseholdMember"
        ],
        "preferredFirstName": "FAKE_First",
        "preferredSurname": "FAKE_Last",
        "preferredTitle": "Reverend",
        "surname": "FAKE_Last",
        "tenures": [
            {
                "id": str(uuid.uuid4()),
                "assetFullAddress": "2 Fake Road, N16 1AA",
                "assetId": str(uuid.uuid4()),
                "endDate": None,
                "paymentReference": str(random.randint(10 ** 10, 10 ** 11 - 1)),
                "propertyReference": str(random.randint(10 ** 7, 10 ** 8 - 1)),
                "startDate": "2013-12-23",
                "type": "Secure",
                "uprn": "100021063882"
            },
            {
                "id": str(uuid.uuid4()),
                "assetFullAddress": "75 Fake Road, E5 1AA",
                "assetId": str(uuid.uuid4()),
                "endDate": "2012-10-26",
                "paymentReference": str(random.randint(10 ** 10, 10 ** 11 - 1)),
                "propertyReference": str(random.randint(10 ** 7, 10 ** 8 - 1)),
                "startDate": "2012-04-19",
                "type": "Temp Annex",
                "uprn": str(random.randint(10 ** 12, 10 ** 13 - 1))
            },
            {
                "id": str(uuid.uuid4()),
                "assetFullAddress": "15 Fake Road N16 1AA",
                "assetId": str(uuid.uuid4()),
                "endDate": None,
                "paymentReference": str(random.randint(10 ** 10, 10 ** 11 - 1)),
                "propertyReference": str(random.randint(10 ** 7, 10 ** 8 - 1)),
                "startDate": "1997-07-24T00:00:00.0000000Z",
                "type": "Leasehold (RTB)",
                "uprn": str(random.randint(10 ** 12, 10 ** 13 - 1))
            }
        ],
        "title": "Reverend",
        "versionNumber": 1
    }
