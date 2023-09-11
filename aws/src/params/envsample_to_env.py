from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

with open("envs.env", "r") as f:
    envs = f.readlines()

stage = Stage.HOUSING_DEVELOPMENT

ssm = generate_aws_service("ssm", stage, "client")

ENVS = {}

for env in envs:
    env = env.strip()
    if env.startswith("#") or not env:
        continue
    stage_value = stage.value.replace("housing-", "")
    env = env.replace("{environment}", stage_value)
    key, value = env.split("=")
    key = key.strip().upper()
    value = value.strip()
    try:
        env = ssm.get_parameter(Name=value, WithDecryption=True)
        ENVS[key] = env['Parameter']['Value'].replace("\n", "")
    except ssm.exceptions.ParameterNotFound:
        ENVS[key] = value

    with open("env_output.env", "w") as f:
        for key, value in ENVS.items():
            f.write(f"{key}={value}\n")


