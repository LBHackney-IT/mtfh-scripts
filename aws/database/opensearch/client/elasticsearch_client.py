import elasticsearch
from elasticsearch import Elasticsearch

import urllib3

# Suppress warnings about insecure connections - this is because we're connecting to localhost and not using SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class LocalElasticsearchClient:
    def __init__(self, index: str | None, port: int = 3333):
        """
        Create a connection to a local Elasticsearch instance
        :param port: Port to connect to
        :param index: Index to connect to
        """
        self.port = port
        self.es_instance = Elasticsearch(
            hosts=[{"host": "localhost", "port": port}],
            use_ssl=True,
            verify_certs=False,
            index=index,
        )
        self._check_connection()
        self._index = index

        if not index:
            return

        if not self.es_instance.indices.exists(index):
            raise ValueError(
                f"Index {index} does not exist. Valid indices are {self.list_all_indices()}"
            )

    def get(self, doc_id: str) -> dict:
        """Return a document in an index by its ID"""
        return self.es_instance.get(index=self._index, id=doc_id)

    def query(self, query: dict, size: int = 1000) -> list:
        """Return all documents in an index matching a query"""
        query = {"query": query}
        res = self.es_instance.search(  # pylint: disable=E1123
            index=self._index, body=query, size=size
        )
        print(
            f"Found {len(res['hits']['hits'])} documents in {self._index} out of {res['hits']['total']['value']}"
        )
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

    def set_attribute(self, doc_id: str, attribute: str, value: str | dict[str, str]):
        """Set an attribute for a document in an index - use a dict for nested properties"""
        self.es_instance.update(
            index=self._index, id=doc_id, body={"doc": {attribute: value}}
        )

    def delete_attribute(self, doc_id: str, attribute: str):
        """Delete an attribute for a document in an index"""
        self.es_instance.update(
            index=self._index,
            id=doc_id,
            body={"script": f"ctx._source.remove('{attribute}')"},
        )

    def list_all_indices(self) -> list[str]:
        """Return all indices in the Elasticsearch instance"""
        return list(self.es_instance.indices.get_alias(index="*").keys())

    def _check_connection(self):
        try:
            self.es_instance.info()
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.ConnectionError:
            print(
                f"Could not connect to ES at localhost:{self.port} - are you port forwarding?"
            )

    def create_index(self, new_index_name):
        """Create a new index"""
        self.es_instance.indices.create(index=new_index_name)

    def delete_index(self, index_name: str):
        """Delete an index"""
        self.es_instance.indices.delete(index=index_name)
