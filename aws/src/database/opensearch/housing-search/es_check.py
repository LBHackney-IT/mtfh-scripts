import elasticsearch
from elasticsearch import Elasticsearch


class LocalElasticsearchClient:
    def __init__(self, port: int, index: str):
        """
        Create a connection to a local Elasticsearch instance
        :param port: Port to connect to
        :param index: Index to connect to
        """
        self.port = port
        self.es_instance = Elasticsearch(f"https://localhost:{port}", verify_certs=False, index=index)
        self._check_connection()
        self.index = index

        # Check if index exists
        if not self.es_instance.indices.exists(index):
            raise ValueError(f"Index {index} does not exist. Valid indices are {self.list_all_indices()}")

    def query(self, query: dict, size: int = 1000) -> list:
        """Return all documents in an index matching a query"""
        query = {"query": query}
        res = self.es_instance.search(index=self.index, body=query, size=size)
        print(f"Found {len(res['hits']['hits'])} documents in {self.index} out of {res['hits']['total']['value']}")
        return res["hits"]["hits"]

    def update(self, doc_id: str, body: dict):
        """Update a document in an index"""
        self.es_instance.update(index=self.index, id=doc_id, body={"doc": body})

    def match_all(self, size: int = 1000) -> list:
        """Return all documents in an index up to the size limit"""
        query = {"match_all": {}}
        return self.query(query, size)

    def get_by_attribute(self, attribute: str, value: str, size=1000) -> list:
        """Return all documents in an index with a given attribute value"""
        query = {
            "match": {
                attribute: {
                    "query": value,
                    "fuzziness": "AUTO",
                    "operator": "and",
                }
            }
        }
        return self.query(query, size)

    def set_attribute(self, index: str, doc_id: str, attribute: str, value: str):
        """Set an attribute for a document in an index"""
        self.es_instance.update(index=index, id=doc_id, body={"doc": {attribute: value}})

    def delete_attribute(self, index: str, doc_id: str, attribute: str):
        """Delete an attribute for a document in an index"""
        self.es_instance.update(index=index, id=doc_id, body={"script": f"ctx._source.remove('{attribute}')"})

    def list_all_indices(self) -> list:
        """Return all indices in the Elasticsearch instance"""
        return list(self.es_instance.indices.get_alias("*").keys())

    def _check_connection(self):
        try:
            self.es_instance.info()
        except elasticsearch.exceptions.ConnectionError:
            print(f"Could not connect to ES at localhost:{self.port} - are you port forwarding?")


def main():
    process_id = "bdc077e3-4c2e-42a5-b590-f28499623312"
    local_es = LocalElasticsearchClient(3333, "processes")
    # assert len(res) == 1
    # process = res[0]["_source"]
    # new_targetId = "35ffe2a1-ee7f-c09d-df11-c065d97fa7d0"
    # process["targetId"] = new_targetId
    # local_es.update(process_id, process)
    #
    local_es.delete_attribute("processes", process_id, "targetid")

    res = local_es.get_by_attribute("id", process_id)
    print(res)


if __name__ == "__main__":
    main()
