from elasticsearch_client import LocalElasticsearchClient


def elastic_search():
    es_client = LocalElasticsearchClient("persons")
    res = es_client.query({"match_all": {}}, size=10)
    print([f'{person["_source"]["firstname"]} {person["_source"]["surname"]}' for person in res])


if __name__ == "__main__":
    elastic_search()
