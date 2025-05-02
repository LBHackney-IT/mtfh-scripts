
# # from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
# # from enums.enums import Stage


# # def main():
# #     table = get_dynamodb_table(
# #         table_name="Persons",
# #         stage=Stage.HOUSING_DEVELOPMENT)
# #     scan = table.scan()
# #     if "LastEvaluatedKey" in scan:
# #         last_evaluated_key = scan["LastEvaluatedKey"]["id"]
# #     print(scan)


# # if __name__ == "__main__":
# #     main()
# #     table = get_dynamodb_table(
# #         table_name="Persons",
# #         stage=Stage.HOUSING_DEVELOPMENT
# #     )

# # from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
# # from enums.enums import Stage


# # def main():
# #     table = get_dynamodb_table(
# #         table_name="Persons",
# #         stage=Stage.HOUSING_DEVELOPMENT)

# #     scan = table.scan()

# #     # Start PersonRef at 70000000
# #     next_person_ref = 70000000

# #     # Loop through scanned items and assign PersonRef
# #     for item in scan.get('Items', []):
# #         table.update_item(
# #             Key={'Id': item['Id']},
# #             UpdateExpression="SET PersonRef = :val",
# #             ExpressionAttributeValues={':val': next_person_ref}
# #         )
# #         print(f"Assigned PersonRef {next_person_ref} to Id {item['Id']}")
# #         next_person_ref += 1

# #     if "LastEvaluatedKey" in scan:
# #         last_evaluated_key = scan["LastEvaluatedKey"]["id"]

# #     print(scan)


# # if __name__ == "__main__":
# #     main()

# #     table = get_dynamodb_table(
# #         table_name="Persons",
# #         stage=Stage.HOUSING_DEVELOPMENT
# #     )

from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from enums.enums import Stage


def process_scan(table, scan, next_person_ref):
    for item in scan.get('Items', []):
        table.update_item(
            Key={'id': item['id']},
            UpdateExpression="SET PersonRef = :val",
            ExpressionAttributeValues={':val': next_person_ref}
        )
        print(f"Assigned PersonRef {next_person_ref} to id {item['id']}")
        next_person_ref += 1

    # Return LastEvaluatedKey (if any) and the next available PersonRef value
    return scan.get('LastEvaluatedKey'), next_person_ref


def main():
    table = get_dynamodb_table(
        table_name="Persons",
        stage=Stage.HOUSING_DEVELOPMENT
    )

    next_person_ref = 70000000
    scan = table.scan()

    while True:
        last_evaluated_key, next_person_ref = process_scan(table, scan, next_person_ref)

        if last_evaluated_key:
            scan = table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            break

    print("All done!")


if __name__ == "__main__":
    main()
