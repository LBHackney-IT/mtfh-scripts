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
        self._index = index

        # Check if index exists
        if not self.es_instance.indices.exists(index):
            raise ValueError(f"Index {index} does not exist. Valid indices are {self.list_all_indices()}")

    def get(self, doc_id: str) -> dict:
        """Return a document in an index by its ID"""
        return self.es_instance.get(index=self._index, id=doc_id)

    def query(self, query: dict, size: int = 1000) -> list:
        """Return all documents in an index matching a query"""
        query = {"query": query}
        res = self.es_instance.search(index=self._index, body=query, size=size)
        print(f"Found {len(res['hits']['hits'])} documents in {self._index} out of {res['hits']['total']['value']}")
        return res["hits"]["hits"]

    def index(self, doc_id: str, body: dict):
        """Put a document in an index"""
        self.es_instance.index(index=self._index, id=doc_id, body=body)

    def update(self, doc_id: str, body: dict):
        """Update a document in an index"""
        self.es_instance.update(index=self._index, id=doc_id, body={"doc": body})

    def delete(self, doc_id: str):
        """Delete a document in an index"""
        self.es_instance.delete(index=self._index, id=doc_id)

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
