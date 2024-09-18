from elasticsearch import Elasticsearch


def update_tenure():
    index = "tenures"
    tenure_id = "82f1519f-5c15-4948-9744-f2f1112fabaa"
    new_prn = "3750340212"
    es = Elasticsearch(
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=True,
        verify_certs=False,
    )

    info = es.info()
    print(info)

    tenure = es.get(index, id=tenure_id)
    assert tenure["_source"].get("paymentReference") is None

    es.update(index, tenure_id, body={"doc": {"paymentReference": new_prn}})

    tenure = es.get(index, id=tenure_id)
    assert tenure["_source"]["paymentReference"] == new_prn
