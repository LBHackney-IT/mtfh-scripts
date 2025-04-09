from typing import TypedDict

from boto3 import Session
from aws.authentication.generate_aws_resource import get_session_for_stage
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from enums.enums import Stage
from mypy_boto3_apigateway import APIGatewayClient


class AccMapSession(TypedDict):
    profile: str
    session: Session


ACC_MAP: dict[str, AccMapSession] = {
    "282997303675": {
        "profile": "housing-production",
        "session": get_session_for_stage(Stage.HOUSING_PRODUCTION),
    },
    "153306643385": {
        "profile": "production-apis",
        "session": get_session_for_stage(Stage.PRODUCTION_APIS),
    },
}


auth_table = get_dynamodb_table("APIAuthenticatorData", Stage.PRODUCTION_APIS)


def main():
    auth_items = auth_table.scan()["Items"]

    for aws_account, details in ACC_MAP.items():
        api_gateway: APIGatewayClient = details["session"].client("apigateway")
        api_gateway_items = api_gateway.get_rest_apis(
            limit=500,
        )["items"]
        related_auth_items = [
            auth_item
            for auth_item in auth_items
            if auth_item["awsAccount"] == aws_account
        ]
        for auth_item in related_auth_items:
            api_name = auth_item["apiName"]

            # Find the api ID
            api_gateway_items_filtered = [
                item for item in api_gateway_items if item["name"] in api_name
            ]
            api_item = None
            if len(api_gateway_items_filtered) == 0:
                print(
                    f"API Gateway item not found for {api_name} in account {aws_account}"
                )
                continue
            elif len(api_gateway_items_filtered) == 1:
                api_item = api_gateway_items_filtered[0]
            else:
                # get user input to select the correct one
                print(
                    f"Multiple API Gateway items found for {api_name} in account {aws_account}: {api_gateway_items_filtered}"
                )
                while True:
                    try:
                        res = input(
                            f"Select the correct one by entering the index (0-{len(api_gateway_items_filtered) - 1}): "
                        )
                        index = int(res)
                        if 0 <= index < len(api_gateway_items_filtered):
                            api_item = api_gateway_items_filtered[index]
                            break
                    except ValueError:
                        pass

            if api_item is None:
                print(
                    f"API Gateway item not found for {api_name} in account {aws_account}"
                )
                continue

            # Update the auth table with the API name to have the correct API ID
            attribute = "apiGatewayId"
            if auth_item.get(attribute) == api_item["id"]:
                print(
                    f"API Gateway ID already set for {api_name} in account {aws_account}: {auth_item[attribute]}"
                )
                continue
            assert (
                input(
                    f"Confirm to update {api_name} in account {aws_account} with API Gateway ID {api_item['id']} (y/n): "
                )
                == "y"
            )
            auth_item[attribute] = api_item["id"]
            auth_table.put_item(Item=auth_item)
            print(
                f"Updated {api_name} in account {aws_account} with API Gateway ID {api_item['id']}"
            )


if __name__ == "__main__":
    main()
T