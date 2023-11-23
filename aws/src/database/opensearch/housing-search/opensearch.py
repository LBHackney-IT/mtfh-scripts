from mypy_boto3_opensearch.client import OpenSearchServiceClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

def opensearch():
    os: OpenSearchServiceClient = generate_aws_service("opensearch", Stage.HOUSING_DEVELOPMENT, "client")
    domains = os.list_domain_names()['DomainNames']
    print([domain['DomainName'] for domain in domains])
    result = os.describe_domain(DomainName="housing-search-api-es")
    print(result)
