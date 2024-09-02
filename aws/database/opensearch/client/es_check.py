from elasticsearch_client import LocalElasticsearchClient


def elastic_search():
    es_client = LocalElasticsearchClient("tenures")
    TENURE_ID = ""

    tenure = es_client.get(TENURE_ID)
    assert tenure["_source"].get("paymentReference") is not None

    es_client.delete(TENURE_ID)


if __name__ == "__main__":
    elastic_search()
