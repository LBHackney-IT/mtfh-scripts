from aws.database.opensearch.client.elasticsearch_client import LocalElasticsearchClient
from utils.confirm import confirm


def find_aliased_indexes(es_client: LocalElasticsearchClient) -> list[str]:
    instance_aliases = es_client.es_instance.indices.get_alias(index='*')
    aliased_indexes: list[str] = []
    for index_name, res in instance_aliases.items():
        index_aliases: dict = res.get("aliases")
        if index_aliases.keys():
            aliased_indexes.append(index_name)
    return aliased_indexes


def storage_for_index(es_client: LocalElasticsearchClient, index: str) -> int:
    stats = es_client.es_instance.indices.stats(index=index, metric='store')
    size_in_bytes = stats['indices'][index]['total']['store']['size_in_bytes']
    return size_in_bytes


def main():
    print("\n\n === ES Start == \n\n")

    es_client = LocalElasticsearchClient(None)

    all_indexes = es_client.list_all_indices()

    indexes_to_preserve = find_aliased_indexes(es_client)
    if not confirm(f"Preserving indexes: {indexes_to_preserve}"):
        return

    indexes_to_clear = [
        index for index in all_indexes
        if index not in indexes_to_preserve
        and "address" in index
    ]
    
    for index_to_clear in indexes_to_clear:
        if index_to_clear in all_indexes:
            size_in_bytes = storage_for_index(es_client, index_to_clear)
            formatted_size = f"{size_in_bytes:_}"
            if confirm(f"Delete index {index_to_clear} with {formatted_size} bytes of data?"):
                es_client.delete_index(index_to_clear)
        else:
            print(f"Index {index_to_clear} not found.")




if __name__ == "__main__":
    main()
