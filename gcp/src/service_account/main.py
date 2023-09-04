
from gcp.src.service_account.get_credentials import get_credentials
from gcp.src.service_account.utils.DriveServiceAccountClient import DriveServiceAccountClient

credentials = get_credentials()
service_account = DriveServiceAccountClient(credentials)



def main():
    # Replace this and write your code below
    folder_id = input("folder to download: ")
    service_account.download_folder_contents(folder_id, "my_drive")
