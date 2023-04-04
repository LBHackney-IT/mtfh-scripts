from typing import Any, Callable

import dacite.exceptions
from dacite import from_dict
from mypy_boto3_dynamodb.service_resource import Table

from aws.src.utils.filter_list_of_dictionaries_by_lambdas import filter_list_of_dictionaries_by_lambdas
from aws.src.utils.logger import Logger


class DynamodbItemFactory:
    def __init__(self, table: Table, output_class: Any, logger: Logger = None):
        self.table = table
        self.output_class = output_class
        self.logger = logger

    def full_extract(self, headings_filters: dict[str:Callable]) -> list[Any]:
        raw_items = []
        if headings_filters is None:
            headings_filters = {"id": lambda x: bool(x)}
        scan = self.table.scan()
        raw_items += filter_list_of_dictionaries_by_lambdas(scan["Items"], headings_filters)
        while "LastEvaluatedKey" in scan:
            self.logger and self.logger.log(f"Scanned {len(scan)} items - last key: {scan['LastEvaluatedKey']}")
            scan = self.table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
            raw_items += filter_list_of_dictionaries_by_lambdas(scan["Items"], headings_filters)
        self.logger and self.logger.log(f"Finished run - scanned {len(scan)} items")
        items = []
        for raw_item in raw_items:
            try:
                item = from_dict(data_class=self.output_class, data=raw_item)
                items.append(item)
            except dacite.exceptions.MissingValueError as e:
                raise TypeError(f"Error converting raw item to {self.output_class.__name__} - {e}")
            except dacite.exceptions.WrongTypeError as e:
                raise TypeError(f"Error converting raw item to {self.output_class.__name__} - {e}")
        return items

    def get_item(self, key: str, value: str) -> Any:
        response = self.table.get_item(Key={key: value})
        try:
            raw_item = response["Item"]
        except KeyError:
            raise ValueError(f"Could not find item with {key} of {value} in table {self.table.name}")

        try:
            item = from_dict(data_class=self.output_class, data=raw_item)
        except dacite.exceptions.MissingValueError as e:
            raise TypeError(f"Error converting raw item to {self.output_class.__name__} - {e}")
        except dacite.exceptions.WrongTypeError as e:
            raise TypeError(f"Error converting raw item to {self.output_class.__name__} - {e}")
        return item
