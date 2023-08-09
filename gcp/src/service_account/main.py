"""
For automating the service account
"""

from gcp.src.service_account.config import Folders
from gcp.src.service_account.get_credentials import get_credentials
from gcp.src.service_account.utils.DriveServiceAccountClient import DriveServiceAccountClient

def main():
    """
    Run
    """
    creds = get_credentials()
    service_account = DriveServiceAccountClient(creds)

    # Replace this and write your code below
    # Example: Gets a file by its ID from user input
    rent_position_folder = service_account.get_file_or_folder(Folders.RENT_POSITION_DEV)
    service_account.delete_matching_files_in_folder(
        rent_position_folder.get('id'), file_size_minimum=1, exclude_latest=True
    )

