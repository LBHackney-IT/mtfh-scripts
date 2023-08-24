
from unittest.mock import MagicMock

import pytest
import aws.src.params.env_generator as env_generator_module
from aws.src.params.env_generator import env_generator


env_generator_module.ssm_client.get_parameter = MagicMock(return_value={"Parameter": {"Value": "test-value"}})

EXPECTED_RESULT = """
PARAM_1="test-value"
PARAM_2="test-value"
""".strip().replace(" ", "")


def test_env_generator_circleci(circleci_env):
    env_res = env_generator(circleci_env)
    assert env_res == EXPECTED_RESULT


def test_env_generator_serverless(serverless_env):
    env_res = env_generator(serverless_env)
    assert env_res == EXPECTED_RESULT


def test_env_generator_terraform(terraform_env):
    env_res = env_generator(terraform_env)
    assert env_res == EXPECTED_RESULT


@pytest.fixture
def circleci_env():
    return """
    export PARAM_1=$(aws ssm get-parameter --name /path/to/<<parameters.stage>>/parameter --query Parameter.Value --output text)
    export OTHER_PARAM=test-value
    export PARAM_2=$(aws ssm get-parameter --name /path/to/<<parameters.stage>>/parameter-two --query Parameter.Value --output text)
    """


@pytest.fixture
def serverless_env():
    return """
    PARAM_1: ${ssm:/path/to/${self:provider.stage}/parameter}
    OTHER_PARAM: test-value
    PARAM_2: ${ssm:/path/to/${self:provider.stage}/parameter-two}
    """


@pytest.fixture
def terraform_env():
    return """
    data "aws_ssm_parameter" "param_1" {
      name = "/path/${var.environment_name}/to/parameter"
    }
    data "aws_ssm_parameter" "param_2" {
      name = "/path/${var.environment_name}/to/parameter-two"
    }
    """


if __name__ == "__main__":
    pass