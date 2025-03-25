from boto3.session import Session
from mypy_boto3_rds import RDSClient

session = Session(profile_name="housing-production-readonly")


"""
Script to compare 2 rds instances, all config and changes between them
"""


rds_1 = "housing-finance-sql-db-production"
rds_2 = "housing-finance-sql-db-production-corrupted"

client: RDSClient = session.client("rds")


def get_rds_config(rds_name: str):
    response = client.describe_db_instances(DBInstanceIdentifier=rds_name)
    return response.get("DBInstances", [])[0]


def compare_rds(rds_1: str, rds_2: str):
    rds_1_config = get_rds_config(rds_1)
    rds_2_config = get_rds_config(rds_2)
    print(f"Key\t{rds_1}\t{rds_2}")
    for key, value in rds_1_config.items():
        if rds_2_config.get(key) != value:
            print(f"{key}\t{value}\t{rds_2_config.get(key)}")


def main():
    compare_rds(rds_1, rds_2)


if __name__ == "__main__":
    main()
