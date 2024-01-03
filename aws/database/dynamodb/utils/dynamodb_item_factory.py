from typing import Any, Callable

from mypy_boto3_dynamodb.service_resource import Table

from aws.utils.safe_object_from_dict import safe_object_from_dict
from aws.utils.filter_list_of_dictionaries_by_lambdas import filter_list_of_dictionaries_by_lambdas
from aws.utils.logger import Logger


class DynamodbItemFactory:
    def __init__(self, table: Table, output_class: Any = None, logger: Logger = None):
        """
        A class to extract items from a DynamoDB table, and convert them to a dataclass
        :param table: DynamoDB table object to extract from
        :param output_class: Dataclass to convert items to
        :param logger: Logger object to log to
        """
        self.table = table
        self.output_class = output_class
        self.logger = logger or Logger()

    def full_extract(self, headings_filters: dict[str:Callable] = None, item_limit: int = None) -> list[Any]:
        """
        Extracts all items from a DynamoDB table, and converts them to the output class if specified.
        Logs all headings in the table.
        :param headings_filters: Dictionary of filters to apply to the headings. Keys are the heading names, values are the filters to apply
        :param item_limit: Will not continue extracting items if this limit is reached
        :return:
        """

        headings_filters = {} if headings_filters is None else headings_filters
        raw_items = self._get_all_items(headings_filters, item_limit)
        self._write_table_headings(raw_items, item_limit)
        if self.output_class is None:
            return raw_items
        items = []
        for raw_item in raw_items:
            item = safe_object_from_dict(self.output_class, raw_item)
            items.append(item)
        return items

    def get_item(self, key: str, value: str) -> Any:
        """
        Gets a single item from the table, and converts it to the output class if specified
        :param key: Primary key of the item to get
        :param value: Value of the primary key
        :return:
        """
        response = self.table.get_item(Key={key: value})
        try:
            raw_item = response["Item"]
        except KeyError:
            raise ValueError(f"Could not find item with {key} of {value} in table {self.table.name}")

        item = safe_object_from_dict(self.output_class, raw_item)
        return item

    def _get_all_items(self, headings_filters: dict[str:Callable] = None, item_limit: int = None):
        raw_items = []
        if headings_filters is None:
            headings_filters = {}
        scan = self.table.scan()
        raw_items += filter_list_of_dictionaries_by_lambdas(scan["Items"], headings_filters)
        item_limit_check = len(raw_items) < item_limit if item_limit else True
        while "LastEvaluatedKey" in scan and item_limit_check:
            self.logger.log(f"Scanned {len(scan)} items - last key: {scan['LastEvaluatedKey']}")
            scan = self.table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
            raw_items += filter_list_of_dictionaries_by_lambdas(scan["Items"], headings_filters)
        self.logger.log(f"Finished run - extracted {len(raw_items)} items")
        return raw_items

    def _write_table_headings(self, raw_items: list[dict] = None, item_limit: int = None):
        """
        Writes all headings in the table to the log
        :param raw_items: List of raw items (from DB export) to extract headings from
        :param item_limit: Limit on number of items to extract
        """
        if raw_items is None:
            raw_items = self._get_all_items(item_limit=item_limit)
        headings: dict[str:str] = {}
        for raw_item in raw_items:
            item_headings = set(raw_item.keys())
            for item_heading in item_headings:
                if item_heading not in headings.keys():
                    headings[item_heading] = type(raw_item[item_heading]).__name__
        self.logger.log(f"Table headings: {headings}")

        if self.output_class:
            unused_heading_keys = set(headings.keys()) - set(self.output_class.__annotations__.keys())
            unused_headings = {key: headings[key] for key in unused_heading_keys}
            self.logger.log(f"Unused headings in class {self.output_class.__name__}: {unused_headings}")
