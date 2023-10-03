from dataclasses import dataclass
from dacite import from_dict

import requests
from json import JSONDecodeError
import os

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

STAGE = Stage.HOUSING_STAGING

_ssm = generate_aws_service('ssm', STAGE, "client")
_address_api_url_path = f"/housing-tl/{STAGE.to_env_name()}/address-api-url"
ADDRESS_API_URL = _ssm.get_parameter(Name=_address_api_url_path)['Parameter']['Value']


def add_failed_uprn(uprn: int, outfile="failed_uprns.csv"):
    with open(outfile, "a") as f:
        f.write(f"{uprn}\n")


@dataclass
class Address:
    line1: str
    line2: str
    line3: str
    line4: str
    town: str
    postcode: str
    UPRN: int


def llpg_address_for_uprn(address_api_url: str, uprn: int) -> Address | None:
    """
    Get an address from the LLP Gazetteer API for a given UPRN
    Requires a hackneyToken environment variable to be set with a valid JWT token
    :param address_api_url: URL of the address API
    :param uprn: UPRN to get address for
    """
    token = os.environ["hackneyToken"]

    # Step 1: Get address(es) for UPRN
    response = requests.get(
        f"{address_api_url}/addresses",
        params={
            "address_scope": "Hackney Gazetteer",
            "uprn": uprn
        },
        headers={
            "authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
    )
    if response.status_code != 200:
        print(f"WARNING: {response.status_code} response when getting address for UPRN {uprn}")
        return None

    # Step 2: Get first valid address from response (if any) and return it as an Address object
    addresses_list: list[dict] | None = response.json().get("data").get("address")

    try:
        address_json = addresses_list[0]
        assert int(address_json["uprn"]) == uprn
        address = from_dict(data_class=Address, data=address_json)
        return address
    except (IndexError, KeyError, AssertionError):
        print(f"WARNING: Could not get address for UPRN {uprn}, got: {addresses_list}")
        add_failed_uprn(uprn)
        return None


def main():
    # uprn = input("Please enter a UPRN: ")
    uprn = int(os.environ["test_uprn"])
    address = llpg_address_for_uprn(ADDRESS_API_URL, uprn)
    print(address)

    #TODO (proposed steps):
    # 1. Load an input source of addresses with UPRNs to update, including an "updated" column
    # 2. For each UPRN, get the address from the address API
    # 3. Get the same address from the assets Dynamo table
    # 4. Update Dynamo if the addresses are similar but not identical, Warn if they are very different
    # 5. Update the "updated" column in the input source to indicate whether the address was updated or not

if __name__ == "__main__":
    main()
