from elasticsearch import Elasticsearch


def elastic_search():
    es = Elasticsearch("https://localhost:3333", verify_certs=False)
    res = es.search(index="staff", body={"query": {"match_all": {}}})["hits"]["hits"]
    print([f'{person["_source"]["firstName"]} {person["_source"]["lastName"]}' for person in res])


if __name__ == "__main__":
    elastic_search()
