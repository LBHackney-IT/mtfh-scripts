# TODO: Uncomment the method and import you want to use
# from aws.src.database.dynamodb.scripts.asset_table.patch_asset_with_additional_data import main as patch_asset_with_additional_data_main
# from aws.src.database.dynamodb.scripts.asset_table.get_assets_by_prop_ref import main as get_assets_by_prop_ref_main
# from aws.src.database.dynamodb.scripts.asset_table.add_patches_data import main as add_patches_data_dynamodb_main
# from aws.src.database.dynamodb.scripts.asset_table.get_assets import main as get_assets_dynamodb_main
# from aws.src.database.dynamodb.scripts.person_table.get_housing_officers_and_area_managers import \
#     main as get_housing_officers_and_area_managers_main
# from aws.src.database.multi_table.amend_tenure import main as amend_tenure_main
# from aws.src.database.multi_table.find_persons_for_properties import main as find_persons_for_properties_main
# from aws.src.database.dynamodb.scripts.contact_details_table.scan_and_patch_new_phone_numbers import main as scan_and_patch_new_phone_numbers_main
#from aws.src.database.dynamodb.scripts.asset_table.patch_asset_with_additional_boiler_house_id import main as update_boilerhouse
# from gcp.src.service_account.main import main as service_account_main
from aws.src.database.dynamodb.scripts.asset_table.get_assets_add_patchId_areaId import main as get_assets_add_patchId_areaId

if __name__ == "__main__":
    # 1) IMPORTANT: Uncomment and click into functions in the imports to view definitions and edit config
    #     - e.g. set STAGE to "development" or "production"
    # 2) Call functions here
    get_assets_add_patchId_areaId()
    
    pass
