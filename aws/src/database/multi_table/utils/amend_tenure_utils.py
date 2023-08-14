import json

import pyperclip

from aws.src.utils.confirm import confirm


def query_item_by_pk_es(query_path: str, es_domain: str, es_extension: str, primary_key: str):
    with open(query_path, "r") as outfile:
        es_query_cmd = outfile.read()
    es_query = json.dumps({"query": {"term": {"id.keyword": primary_key}}})
    curl_query = f"curl -X GET '{es_domain}/{es_extension}/_search' -H 'Content-Type: application/json' " \
                 f"-d '{es_query}' | python -c '\n{es_query_cmd}'"
    print("== Elasticsearch Query ==")
    print(curl_query)
    pyperclip.copy(curl_query)
    confirm(f"Command to query ES copied to clipboard. Correct {es_extension}? ")


def update_item_by_pk_es(es_domain: str, es_extension: str, primary_key: str, update_obj: str):
    curl_update = f"curl -X POST '{es_domain}/{es_extension}/_update/{primary_key}'" \
                  f" -H 'Content-Type: application/json' -d '{update_obj}'"
    pyperclip.copy(curl_update)

    print(f"== {es_extension} Elasticsearch Update ==")
    print(update_obj)
    confirm(f"Command to update {es_extension[0:-1]} copied to clipboard.\nElasticsearch updated?")

    print(f"Elasticsearch updated!")

def delete_item_by_pk_es(es_domain: str, es_extension: str, primary_key: str):
    curl_delete = f"curl -L -X DELETE '{es_domain}/{es_extension}/_doc/{primary_key}?pretty=true'"
    pyperclip.copy(curl_delete)

    print(f"== {es_extension} Elasticsearch Item Deletion ==")
    print(primary_key)
    confirm(f"Command to delete {es_extension[0:-1]} copied to clipboard.\nElasticsearch updated?")

    print(f"Elasticsearch updated!")


def connect_to_jumpbox_for_es(instance_id: str, stage: str):
    command = f"aws ssm start-session --target {instance_id} --region eu-west-2 --profile {stage};\n"
    pyperclip.copy(command)

    print(
        "\n== Connect to Jumpbox/Bastion for ElasticSearch ==\n",
        command,
        "\n== ==\n")
    confirm(f"Command to connect to Jumpbox/Bastion copied to clipboard. Connected?")
