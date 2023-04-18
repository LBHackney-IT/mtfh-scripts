from config import Folders
from get_credentials import CREDENTIALS
from utils.DriveServiceAccountClient import DriveServiceAccountClient

service_account = DriveServiceAccountClient(CREDENTIALS)

if __name__ == '__main__':
    # Replace this and write your code below
    # Example: Gets a file by its ID from user input
    rent_position_folder = service_account.get_file_or_folder(Folders.RENT_POSITION)
    service_account.delete_matching_files_in_folder(rent_position_folder.get('id'), file_size_minimum=1)
