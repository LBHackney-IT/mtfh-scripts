from aws.src.database.scripts.person_table.get_housing_officers_and_area_managers import logger
from aws.src.enums.enums import Stage
from aws.src.utils.filter_list_of_dictionaries_by_lambdas import filter_list_of_dictionaries_by_lambdas
from aws.src.database.utils.get_dynamodb_table import get_dynamodb_table


def filter_and_write_to_csv(items, outfile, headings_filters):
    filtered_items = filter_list_of_dictionaries_by_lambdas(items, headings_filters)
    csv_rows = [",".join(f",{obj[key]}" for key in headings_filters.keys()) for obj in filtered_items]
    outfile.writelines(csv_rows)
    logger.log(f"Scanned {len(csv_rows)} items")


def dynamodb_to_csv(table_name: str, stage: Stage, output_directory: str, headings_filters: dict[str, any]):
    dynamo_table = get_dynamodb_table(table_name, stage)
    with open(f"{output_directory}_{stage}.csv", "w") as outfile:
        outfile.write(",".join(headings_filters.keys()))
        scan = dynamo_table.scan()
        filter_and_write_to_csv(scan["Items"], outfile, headings_filters)
        while "LastEvaluatedKey" in scan:
            logger.log(f"Scanned {len(scan)} rows - last key: {scan['LastEvaluatedKey']}")
            scan = dynamo_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])["Items"]
            filter_and_write_to_csv(scan, outfile, headings_filters)

        logger.log(f"Finished run - scanned {len(scan)} rows")
