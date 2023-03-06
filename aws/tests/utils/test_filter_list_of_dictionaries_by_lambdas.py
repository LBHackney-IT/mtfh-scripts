import pytest
from src.utils.filter_list_of_dictionaries_by_lambdas import filter_list_of_dictionaries_by_lambdas


class TestFilterListOfDictionariesByLambdas:
    def test_filter_list_of_dictionaries_by_lambdas(self):
        input_list = [
            {
                "id": "1",
                "firstName": "John",
                "surname": "Smith",
                "personTypes": ["Tenant", "HousingOfficer"],
            },
            {
                "id": "2",
                "firstName": "Jane",
                "surname": "Doe",
                "personTypes": ["Tenant", "HousingAreaManager"],
            },
            {
                "id": "3",
                "firstName": "Bob",
                "surname": "Smith",
                "personTypes": ["Tenant"],
            },
        ]
        headings_filters = {
            "personTypes": lambda x: any(y in ["HousingAreaManager", "HousingOfficer"] for y in x),
        }
        expected = [
            {
                "id": "1",
                "firstName": "John",
                "surname": "Smith",
                "personTypes": ["Tenant", "HousingOfficer"],
            },
            {
                "id": "2",
                "firstName": "Jane",
                "surname": "Doe",
                "personTypes": ["Tenant", "HousingAreaManager"],
            },
        ]
        actual = filter_list_of_dictionaries_by_lambdas(input_list, headings_filters)
        assert expected == actual
