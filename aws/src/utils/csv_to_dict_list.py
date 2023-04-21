import csv
import json


def csv_to_dict_list(file_path: str) -> list[dict]:
    """
    Load a csv file into a list of dicts - one dict per row
    Assumes that the first row of the csv file is a header row
    Tries to decode any json into lists or dicts
    :param file_path: Path to csv file to load
    :return: list of a dict for each row in the csv file
    """
    result_list = []
    with open(file_path) as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            result_list.append(dict(row))

    for result in result_list:
        for key in result.keys():
            try:
                result[key] = json.loads(result[key])
            except json.JSONDecodeError:
                pass

    return result_list


def dict_list_to_csv_rows(dict_list: list[dict], header_row: list[str]) -> list[list[str]]:
    csv_rows = []
    for i, dict_item in enumerate(dict_list):
        csv_row = []
        for header in header_row:
            csv_row.append(dict_item[header])
        csv_rows.append(csv_row)
    return csv_rows
