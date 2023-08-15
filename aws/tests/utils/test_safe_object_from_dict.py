from _decimal import Decimal

from aws.src.database.domain.dynamo_domain_objects import Tenure, HouseholdMember
from aws.src.utils.safe_object_from_dict import safe_object_from_dict


def test_safe_object_from_dict_generates_tenure():
    """
    Returns a Tenure object with the correct values
    """
    eg_tenure = {
        "id": '4c148ab7-a58b-a4c2-053a-03bc1684b559',
        'householdMembers': [
            {'fullName': 'FAKE_First FAKE_Last', 'isResponsible': True, 'dateOfBirth': '1900-03-02T00:00:00',
             'personTenureType': 'Tenant', 'id': '713ac847-99fb-c771-5a72-55542a76dcc2', 'type': 'Person'}]
    }

    tenure = safe_object_from_dict(Tenure, eg_tenure)

    # Does outer type conversion
    assert isinstance(tenure, Tenure)
    assert tenure.id == '4c148ab7-a58b-a4c2-053a-03bc1684b559'

    # Does inner type conversion
    assert isinstance(tenure.householdMembers[0], HouseholdMember)
    assert tenure.householdMembers[0].fullName == 'FAKE_First FAKE_Last'

def test_safe_object_from_dict_handles_undefined_fields():
    """
    Not affected by undefined fields in outer and inner objects and still allows for inner conversion
    """
    eg_tenure = {
        "id": '4c148ab7-a58b-a4c2-053a-03bc1684b559',
        "undefinedOuterField": "undefined",
        'householdMembers': [
            {'fullName': 'FAKE_First FAKE_Last', 'isResponsible': True, 'dateOfBirth': '1900-03-02T00:00:00',
             'personTenureType': 'Tenant', 'id': '713ac847-99fb-c771-5a72-55542a76dcc2', 'type': 'Person',
             'undefinedInnerField': 'undefined'
            }]
    }

    tenure = safe_object_from_dict(Tenure, eg_tenure)

    # Sets defined fields on outer object
    assert hasattr(tenure, 'id')
    assert hasattr(tenure, 'householdMembers')

    # Does not set undefined fields on outer object
    assert not hasattr(tenure, 'undefinedOuterField')

    # Sets defined fields on inner object
    assert hasattr(tenure.householdMembers[0], 'id')
    assert hasattr(tenure.householdMembers[0], 'fullName')

    # Does not set undefined fields on inner object
    assert not hasattr(tenure.householdMembers[0], 'undefinedInnerField')
