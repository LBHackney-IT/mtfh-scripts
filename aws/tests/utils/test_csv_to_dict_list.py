import pytest

from src.utils.csv_to_dict_list import csv_to_dict_list


class TestCSVToDictList:
    def test_csv_to_dict_list(self):
        input_file_path = "../test_data/test_csv_data.csv"
        expected = [
            {
                'firstName': 'John',
                'id': '1',
                'personTypes': '["Student"]',
                'surname': 'Smith'
            },
            {
                'firstName': 'Jane',
                'id': '2',
                'personTypes': "['Student','Teacher']",
                'surname': 'Smith'
            },
            {
                'firstName': 'Cal',
                'id': '3',
                'personTypes': '["Teacher"]',
                'surname': 'Doe'
            },
        ]
        actual = csv_to_dict_list(input_file_path)
        assert expected == actual
