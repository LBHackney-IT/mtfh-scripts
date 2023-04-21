from aws.src.database.dynamodb.scripts.asset_table.add_patches_data import main as add_patches_data_dynamodb_main
from aws.src.database.dynamodb.scripts.asset_table.get_assets import main as get_assets_dynamodb_main
from aws.src.database.dynamodb.scripts.person_table.get_housing_officers_and_area_managers import \
    main as get_housing_officers_and_area_managers_main
from aws.src.database.multi_table.amend_tenure import main as amend_tenure_main
from aws.src.database.multi_table.find_persons_for_properties import main as find_persons_for_properties_main


def get_assets_dynamodb():
    get_assets_dynamodb_main()


def add_patches_data_dynamodb():
    add_patches_data_dynamodb_main()


def get_housing_officers_and_area_managers():
    get_housing_officers_and_area_managers_main()


def find_persons_for_properties():
    find_persons_for_properties_main()


def amend_tenure():
    amend_tenure_main()


if __name__ == "__main__":
    # 1) IMPORTANT: Click into functions above to view definitions and edit config
    #     - e.g. set STAGE to "development" or "production"
    # 2) Call functions here
    pass
