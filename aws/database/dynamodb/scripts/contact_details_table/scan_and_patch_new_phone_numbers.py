from dataclasses import dataclass

from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_array
from aws.utils.logger import Logger
from enums.enums import Stage
import time
#
# IMPORTANT:
#
# This script uses the scan() operation in DyanmoDB which is very expensive
# if used repeatedly. DO NOT invoke these methods in a loop. Rather, supply
# a list of affected records and run the script once
#

@dataclass
class Config:
    TABLE_NAME = "ContactDetails"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws\src\database\input\\bad_phone_numbers.csv"

total_count = 0
update_count = 0

def process_item(item, bad_phone_numbers, dynamo_table, logger):
    global update_count
    partition_key = item['targetId']
    sort_key = item['id']
    #logger.log(f"Process Item {partition_key} / {sort_key}")
    if not item.get('contactInformation'):
        return
    if not item.get('contactInformation').get('value'):
        return
    if item.get('contactInformation').get('contactType') != 'phone':
        return
    phone = item.get('contactInformation').get('value')
    if phone in bad_phone_numbers:
        logger.log(f"Item {partition_key} / {sort_key} has a bad phone number! Erasing....")
        item["contactInformation"]["value"] = ''
        dynamo_table.put_item(Item=item)
        update_count += 1

def process_scan(scan_results, bad_phone_numbers, dynamo_table, logger):
    global total_count
    global update_count
    for item in scan_results['Items']:
        process_item(item, bad_phone_numbers, dynamo_table, logger)
        total_count += 1
    return

def main():
    global total_count
    global update_count
    dynamo_table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    bad_phone_numbers = csv_to_array(_file_path)

    logger = Config.LOGGER
    scan = dynamo_table.scan()

    process_scan(scan, bad_phone_numbers, dynamo_table, logger)

    while "LastEvaluatedKey" in scan:
        logger.log(f"Scanned {len(scan)} rows - last key: {scan['LastEvaluatedKey']} total: {total_count} updated: {update_count}")
        time.sleep(10)
        scan = dynamo_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        process_scan(scan, bad_phone_numbers, dynamo_table, logger)

    logger.log(f"Operation complete")
    logger.log(f"Records updated: {update_count}")
    logger.log(f"Records rejected: {total_count - update_count}")
    logger.log(f"Total records: {total_count}")
