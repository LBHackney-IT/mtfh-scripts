from _decimal import Decimal

from aws.src.database.domain.dynamo_domain_objects import Tenure, HouseholdMember
from aws.src.utils.safe_object_from_dict import safe_object_from_dict


def test_generate_tenure_from_dict():
    """
    Test that safe_object_from_dict returns a Tenure object with the correct values
    """
    eg_tenure = {
        "id": '4c148ab7-a58b-a4c2-053a-03bc1684b559',
        'householdMembers': [
            {'fullName': 'FAKE_First FAKE_Last', 'isResponsible': True, 'dateOfBirth': '1900-03-02T00:00:00',
             'personTenureType': 'Tenant', 'id': '713ac847-99fb-c771-5a72-55542a76dcc2', 'type': 'Person'}]
    }

    tenure = safe_object_from_dict(Tenure, eg_tenure)

    assert tenure.id == '4c148ab7-a58b-a4c2-053a-03bc1684b559'
    assert tenure.householdMembers[0].fullName == 'FAKE_First FAKE_Last'
    assert isinstance(tenure.householdMembers[0], HouseholdMember)