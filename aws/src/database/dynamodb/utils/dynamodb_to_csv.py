import csv

from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.filter_list_of_dictionaries_by_lambdas import filter_list_of_dictionaries_by_lambdas
from aws.src.utils.logger import Logger
from enums.enums import Stage

logger = Logger()


def filter_and_write_to_csv(items, outfile, headings_filters):
    print(f"Writing to {outfile.name}...")
    filtered_items = filter_list_of_dictionaries_by_lambdas(items, headings_filters)
    csv_rows = []
    for filtered_item in filtered_items:
        csv_row = []
        for key in headings_filters.keys():
            csv_row.append(str(filtered_item[key]))
        csv_rows.append(",".join(csv_row))
    outfile.writelines(csv_rows)
    logger.log(f"Scanned {len(csv_rows)} items")


def dynamodb_to_tsv(table_name: str, stage: Stage, output_directory: str, headings_filters: dict[str, any]):
    dynamo_table = get_dynamodb_table(table_name, stage)
    with open(f"{output_directory}_{stage}.tsv", "w") as outfile:
        writer = csv.writer(outfile, delimiter="\t", lineterminator="\n")
        outfile.write(",".join(headings_filters.keys()) + "\n")
        scan = dynamo_table.scan()
        filter_and_write_to_csv(scan["Items"], outfile, headings_filters)
        while "LastEvaluatedKey" in scan:
            logger.log(f"Scanned {len(scan)} rows - last key: {scan['LastEvaluatedKey']}")
            scan = dynamo_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
            filter_and_write_to_csv(scan["Items"], outfile, headings_filters)

        logger.log(f"Finished run - scanned {len(scan)} rows")
