from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage
from mypy_boto3_lambda import LambdaClient


lambda_client: LambdaClient = generate_aws_service("lambda", Stage.HOUSING_DEVELOPMENT)


def get_lambda_versions(function):
    versions = []
    res = lambda_client.list_versions_by_function(
        FunctionName=function["FunctionName"],
    )
    versions.extend(res["Versions"])
    while "NextMarker" in res:
        res = lambda_client.list_versions_by_function(
            FunctionName=function["FunctionName"],
            Marker=res["NextMarker"],
        )
        versions.extend(res["Versions"])
    return versions


def clean_old_lambda_versions(lambda_function):
    # Clean old Lambda layers
    versions = get_lambda_versions(lambda_function)
    sorted_versions = sorted(versions, key=lambda x: x["LastModified"], reverse=True)
    if len(sorted_versions) <= 10:
        return
    versions_to_delete = sorted_versions[10:]
    assert "$LATEST" not in [
        version["Version"] for version in versions_to_delete
    ], "Cannot delete $LATEST version"
    for version in versions_to_delete:
        lambda_client.delete_function(
            FunctionName=lambda_function["FunctionName"],
            Qualifier=version["Version"],
        )
    print(
        f"Deleted {len(versions_to_delete)} old versions of {lambda_function['FunctionName']}"
    )


def clean_all_lambda_functions():
    all_functions = []
    paginator = lambda_client.get_paginator("list_functions")
    for page in paginator.paginate():
        for function in page["Functions"]:
            all_functions.append(function)

    for function in all_functions:
        clean_old_lambda_versions(function)

    print("Done listing all Lambda functions.")


def main():
    clean_all_lambda_functions()
    # eg_function = lambda_client.get_function(FunctionName="processes-api-development")
    # clean_old_lambda_versions(eg_function["Configuration"])


if __name__ == "__main__":
    main()
