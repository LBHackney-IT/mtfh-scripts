from boto3.session import Session
from mypy_boto3_lambda import LambdaClient


def get_lambda_env_vars(session: Session, function_name: str) -> dict:
    client: LambdaClient = session.client("lambda")
    response = client.get_function_configuration(FunctionName=function_name)
    return response.get("Environment", {}).get("Variables", {})


def query_lambdas(session: Session, prefix: str) -> list:
    client: LambdaClient = session.client("lambda")
    response = client.list_functions()
    lambdas = response.get("Functions", [])
    return [
        lambda_.get("FunctionName")
        for lambda_ in lambdas
        if lambda_.get("FunctionName", "").startswith(prefix)
    ]


def main():
    session = Session(profile_name="housing-staging")
    lambdas = query_lambdas(session, "housing-finance-interim-api")
    for lambda_ in lambdas:
        env_vars = get_lambda_env_vars(session, lambda_)
        print(f"Lambda {lambda_}")


if __name__ == "__main__":
    main()
