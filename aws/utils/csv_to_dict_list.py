import csv
import json
from collections import Counter


def csv_to_dict_list(file_path: str, is_tsv: bool = False) -> list[dict]:
    """
    Load a csv file into a list of dicts - one dict per row
    Assumes that the first row of the csv file is a header row
    Tries to decode any json into lists or dicts
    If a header name appears more than once, its values are collected
    into a list (in column order) instead of overwriting each other
    :param file_path: Path to csv file to load
    :param is_tsv: If True, treat file as TSV (tab-separated values) instead of CSV
    :return: list of a dict for each row in the csv file
    """
    result_list = []
    with open(file_path) as file_obj:
        delimiter = "\t" if is_tsv else ","
        reader = csv.reader(file_obj, delimiter=delimiter)
        header_row = next(reader)
        header_counts = Counter(header_row)

        for row in reader:
            result = {}
            for key, value in zip(header_row, row):
                if header_counts[key] > 1:
                    result.setdefault(key, []).append(value)
                else:
                    result[key] = value
            result_list.append(result)

    for result in result_list:
        for key, value in result.items():
            if isinstance(value, list):
                result[key] = [_try_json_load(v) for v in value]
            else:
                result[key] = _try_json_load(value)

    return result_list


def _try_json_load(value):
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def dict_list_to_csv_rows(
    dict_list: list[dict], header_row: list[str]
) -> list[list[str]]:
    csv_rows = []
    for i, dict_item in enumerate(dict_list):
        csv_row = []
        for header in header_row:
            csv_row.append(dict_item[header])
        csv_rows.append(csv_row)
    return csv_rows


def csv_to_array(file_path: str) -> list[str]:
    with open(file_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        data_array = []
        for row in csv_reader:
            data_array.append(row[0])
        return data_array