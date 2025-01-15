import time

from boto3 import Session
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_stepfunctions import SFNClient
from mypy_boto3_sts import STSClient
from mypy_boto3_ecs import ECSClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from enums.enums import Stage


def lambda_name(stage: Stage, name: str) -> str:
    return f"housing-finance-interim-api-{stage.to_env_name()}-{name}"


def step_function_arn(region: str, account: str, name: str) -> str:
    name = f"HF{name}StateMachine"
    return f"arn:aws:states:{region}:{account}:stateMachine:{name}"


def trigger_lambda(lambda_client: LambdaClient, name: str) -> None:
    response = lambda_client.invoke(FunctionName=name, InvocationType="RequestResponse")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    print(f"Invoked lambda {name} with status: {status}")


def trigger_step_function(step_function_client: SFNClient, arn: str) -> None:
    name = arn.split(":")[-1]
    response = step_function_client.start_execution(stateMachineArn=arn)
    execution_arn = response.get("executionArn")
    print(f"Started execution of step function {name} with ARN: {execution_arn}")

    time_passed = 0
    interval = 10
    while True:
        response = step_function_client.describe_execution(executionArn=execution_arn)
        status = response.get("status")
        time.sleep(interval)
        time_passed += interval
        print(f"Execution status: {status} after {time_passed} seconds")

        if status == "SUCCEEDED":
            print(f"Execution of step function {name} succeeded")
            break
        elif status == "FAILED":
            raise RuntimeError("Execution failed")


def trigger_ecs_task(ecs_client: ECSClient, cluster: str, task_definition: str) -> None:
    response = ecs_client.run_task(cluster=cluster, taskDefinition=task_definition)
    failures = response.get("failures")
    if failures:
        raise RuntimeError(f"Failed to run ECS task: {failures}")
    print(f"Running ECS task: {task_definition}")

    task_arn = response.get("tasks")[0].get("taskArn")
    if task_arn is None:
        raise RuntimeError("Failed to get task ARN")

    time_passed = 0
    interval = 10
    while True:
        response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
        status = response.get("tasks")[0].get("lastStatus")
        time.sleep(interval)
        time_passed += interval
        print(f"Task status: {status} after {time_passed} seconds")

        if status == "STOPPED":
            print(f"ECS task {task_arn} stopped")
            break


def main():
    # Setup
    stage = Stage.HOUSING_DEVELOPMENT
    session = get_session_for_stage(stage)

    lambda_client: LambdaClient = session.client("lambda")
    ecs_client: ECSClient = session.client("ecs")
    step_function_client: SFNClient = session.client("stepfunctions")
    sts_client: STSClient = session.client("sts")
    account = sts_client.get_caller_identity()["Account"]

    def arn_for_name(name: str) -> str:
        return step_function_arn(session.region_name, account, name)

    def cluster_name(stage: Stage) -> str:
        return f"hfs-nightly-jobs-charges-ingest-cluster-{stage.env_name}"

    # Act
    # trigger_lambda(lambda_client, lambda_name(stage, "load-tenagree"))
    trigger_ecs_task(
        ecs_client, cluster_name(stage), "charges-ingest-task-defintion-task"
    )
    # trigger_step_function(step_function_client, arn_for_name("CashFile"))


if __name__ == "__main__":
    main()
