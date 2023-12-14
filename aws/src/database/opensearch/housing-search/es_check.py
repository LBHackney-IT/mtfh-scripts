from elasticsearch import Elasticsearch

from elasticsearch_client import LocalElasticsearchClient


def main():
    es_client = LocalElasticsearchClient(index="nov22-assets", port=3333)
    results = es_client.match_all()

    print(results)


if __name__ == "__main__":
    main()
