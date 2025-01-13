import json
from elasticsearch import Elasticsearch
from utils.confirm import confirm


DUMB_ITEMS = []
with open("assets.json", "r") as f:
    DUMB_ITEMS = json.load(f)


def load_dummy_data():
    if not confirm("Are you sure you want to load dummy data?"):
        return
    index = "assets"
    # create an elasticsearch client
    es = Elasticsearch([{"host": "localhost", "port": 9200}])

    # check connection
    if not es.ping():
        raise ValueError("Connection failed")

    # create index if it does not exist
    if not es.indices.exists(index):
        es.indices.create(index=index)

    # clear all data in the index
    es.delete_by_query(index=index, body={"query": {"match_all": {}}})

    for item in DUMB_ITEMS:
        es.index(index=index, id=item["id"], body=item)


if __name__ == "__main__":
    load_dummy_data()
