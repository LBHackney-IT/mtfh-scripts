from aws.src.database.domain.dynamo_domain_objects import Tenure, HouseholdMember

def test_generates_tenure():
    tenure_dict = {
        "id": '4c148ab7-a58b-a4c2-053a-03bc1684b559',
        'householdMembers': [
            {'fullName': 'FAKE_First FAKE_Last', 'isResponsible': True, 'dateOfBirth': '1900-03-02T00:00:00',
                'personTenureType': 'Tenant', 'id': '713ac847-99fb-c771-5a72-55542a76dcc2', 'type': 'Person'}
        ]
    }

    tenure = Tenure.from_data(tenure_dict)

    assert isinstance(tenure, Tenure)
    assert tenure.id == '4c148ab7-a58b-a4c2-053a-03bc1684b559'
    assert isinstance(tenure.householdMembers[0], HouseholdMember)
    assert tenure.householdMembers[0].fullName == 'FAKE_First FAKE_Last'