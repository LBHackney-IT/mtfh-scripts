from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from enums.enums import Stage


def process_scan(table, scan, next_person_ref):
    for item in scan.get('Items', []):
        table.update_item(
            Key={'id': item['id']},
            UpdateExpression="SET PersonRef = :val",
            ExpressionAttributeValues={':val': next_person_ref}
        )
        print(f"Assigned personRef {next_person_ref} to id {item['id']}")
        next_person_ref += 1

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

    last_assigned_ref = next_person_ref - 1
    with open('last_person_ref.log', 'w') as f:
        f.write(str(last_assigned_ref))

    print(f"Last assigned personRef was: {last_assigned_ref}")
    print("All done!")


if __name__ == "__main__":
    main()
