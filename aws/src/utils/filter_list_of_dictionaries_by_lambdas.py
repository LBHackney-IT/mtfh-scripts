from typing import Callable


def filter_list_of_dictionaries_by_lambdas(input_list: list[dict], headings_filters: dict[str, Callable]) -> list[dict]:
    """
    Filters a list of dictionaries by a dictionary of filters,
    where the keys are the headings and the values are the filters as lambdas.\n
    NB: ONLY the headings provided will be returned in the output list of dictionaries.\n
    Headings filters should be lambda functions with an argument mapped to a boolean expression.\n
    Example headings_filters:\n
    {
        # returns all "truthy" values - excludes None, False, empty lists and strings, etc.\n
        "my_id": lambda x: bool(x),\n
        .\n
        # returns only items where heading "my_name" is "Sherlock"\n
        "my_name": lambda x: x == "Sherlock"\n
        .\n
        # returns only items where "my_colour" is "blue" or "green"\n
        "my_colour": lambda x: x in ["blue", "green"]\n
        .\n
        # returns only items where any iterable values under "my_beverages" are "tea" or "coffee"\n
        "my_beverages": lambda x: any(beverage in ["tea", "coffee"] for beverage in x)\n
    }\n
    :param input_list: list of dictionaries
    :param headings_filters: map of headings to filters as lambdas
    :return: processed list of dictionaries
    """
    try:
        filtered_list = [dictionary for dictionary in input_list if all(
            filter_function(dictionary[heading]) for heading, filter_function in headings_filters.items()
        )]
    except KeyError:
        raise KeyError(
            f"Headings {set(headings_filters.keys()) - set(input_list[0].keys())} not found in input_list"
        )
    return filtered_list
