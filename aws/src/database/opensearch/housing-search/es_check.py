from mypy_boto3_opensearch.client import OpenSearchServiceClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

from elasticsearch import Elasticsearch


def elastic_search():
    es = Elasticsearch("https://localhost:3333", verify_certs=False)
    res = es.search(index="staff", body={"query": {"match_all": {}}})["hits"]["hits"]
    print([f'{person["_source"]["firstName"]} {person["_source"]["lastName"]}' for person in res])


def opensearch():
    os: OpenSearchServiceClient = generate_aws_service("opensearch", Stage.HOUSING_DEVELOPMENT, "client")
    domains = os.list_domain_names()['DomainNames']
    print([domain['DomainName'] for domain in domains])
    res2 = os.describe_domain(DomainName="housing-search-api-es")
    print(res2)


if __name__ == "__main__":
    elastic_search()
