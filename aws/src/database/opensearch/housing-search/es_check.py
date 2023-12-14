import elasticsearch
from elasticsearch import Elasticsearch

from elasticsearch_client import LocalElasticsearchClient


def main():
    es_client = LocalElasticsearchClient(3333, "nov22-assets")
    results = es_client.match_all()

    print(results)


if __name__ == "__main__":
    main()
