from dataclasses import dataclass

import sqlalchemy
from mypy_boto3_rds import RDSClient
from sqlalchemy import select, Executable, URL
from sshtunnel import SSHTunnelForwarder
from aws.src.enums.enums import Stage
from aws.src.authentication.generate_aws_resource import generate_aws_resource

ssm_client = generate_aws_resource("ssm", Stage.BASE_STAGING, "client")


@dataclass
class Config:
    STAGE = Stage.BASE_STAGING
    BASE_LINK = "/uh-api/staging/"
    LOCAL_ENDPOINT = "localhost"
    LOCAL_PORT = 5432
    DB_ENDPOINT = ssm_client.get_parameter(Name=BASE_LINK + "postgres-hostname")["Parameter"]["Value"]
    DB_PORT = ssm_client.get_parameter(Name=BASE_LINK + "postgres-port")["Parameter"]["Value"]
    USER = ssm_client.get_parameter(Name=BASE_LINK + "postgres-username")["Parameter"]["Value"]
    PASSWORD = ssm_client.get_parameter(Name=BASE_LINK + "postgres-password")["Parameter"]["Value"]
    REGION = ssm_client.meta.region_name  # Get region from client object metadata
    DBNAME = "uh_mirror"

    #TODO: Need to set up the port forwarding properly - connect to EC2 instance and then connect to RDS instance


def get_postgres_table(rds_client: RDSClient, stage: Stage):
    with SSHTunnelForwarder(
        (Config.LOCAL_ENDPOINT, Config.LOCAL_PORT),
        ssh_username=Config.USER,
        ssh_password=Config.PASSWORD,
        remote_bind_address=(Config.DB_ENDPOINT, Config.DB_PORT),
    ) as tunnel:
        url_obj = URL.create(
            "postgresql+psycopg2",
            username=Config.USER,
            password=Config.PASSWORD,
            host=Config.DB_ENDPOINT,
            port=tunnel.local_bind_port,
        )
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{Config.USER}:{Config.PASSWORD}@{Config.DB_ENDPOINT}:{tunnel.local_bind_port}/{Config.DBNAME}"
        )
        with engine.connect() as connection:
            cmd: Executable = select(["*"]).where(id=1).select_from(uh_mirror)
            result = connection.execute(cmd)
            for row in result:
                print(row)



if __name__ == "__main__":
    client: RDSClient = generate_aws_resource("rds", Stage.BASE_STAGING, "client")
    get_postgres_table(client, Stage.BASE_STAGING)
