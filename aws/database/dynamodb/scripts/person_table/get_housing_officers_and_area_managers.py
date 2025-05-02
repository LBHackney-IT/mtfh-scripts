# from dataclasses import dataclass

# from aws.database.dynamodb.utils.dynamodb_to_csv import dynamodb_to_tsv
# from enums.enums import Stage


# @dataclass
# class Config:
#     STAGE = Stage.HOUSING_DEVELOPMENT
#     OUTPUT_DIRECTORY = "../../output"
#     HEADINGS_FILTERS = {
#         "id": lambda x: bool(x),
#         "firstName": lambda x: bool(x),
#         "surname": lambda x: bool(x),
#         "personTypes": lambda x: any(person_type in ["HousingAreaManager", "HousingOfficer"] for person_type in x),
#     }


# def main():
#     dynamodb_to_tsv("Persons", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADINGS_FILTERS)


from dataclasses import dataclass
from aws.database.dynamodb.utils.dynamodb_to_csv import dynamodb_to_tsv
from enums.enums import Stage
from aws.database.dynamodb.utils.dynamodb_operations import update_item, scan_table 



def add_person_refs_to_persons(table_name, stage, starting_ref=70000000):
    
    current_ref = starting_ref
    processed = 0
    
    print(f"Adding personRef to records in {table_name}...")
    
    
    all_items = scan_table(table_name, stage)
    
    for item in all_items:
       
        if not item.get('id') or not item.get('firstName') or not item.get('surname'):
            continue
            
        
        
        
        update_item(
            table_name,
            {'id': item['id']},  
            {'personRef': current_ref},  
            stage
        )
        
        current_ref += 1
        processed += 1
        
        if processed % 100 == 0:
            print(f"Updated {processed} records with personRef")
    
    print(f"Completed adding personRef to {processed} records")
    return processed

def main():
    
    records_updated = add_person_refs_to_persons("Persons", Config.STAGE, Config.STARTING_REF)
    print(f"Added personRef to {records_updated} records")
    
    
    updated_filters = Config.HEADINGS_FILTERS.copy()
    updated_filters["personRef"] = lambda x: bool(x)
    
    dynamodb_to_tsv("Persons", Config.STAGE, Config.OUTPUT_DIRECTORY, updated_filters)

if __name__ == "__main__":
    main()