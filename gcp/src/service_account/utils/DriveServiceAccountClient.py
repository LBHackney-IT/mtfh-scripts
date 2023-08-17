import io
import json
import os
import re

from google.oauth2 import service_account
from googleapiclient import errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from aws.src.utils.progress_bar import ProgressBar


def confirm(prompt: str) -> bool:
    """
    Prompts user to confirm an action
    :param prompt: Prompt to show user
    :return: True if user confirms, False if user denies
    """
    while True:
        response = input(f"{prompt} (y/n): ")
        if response.lower() in ["y", "yes"]:
            return True
        elif response.lower() in ["n", "no"]:
            return False
        else:
            print("Invalid response. Please enter y or n.")



class DriveServiceAccountClient:
    """
    To use, you must get the credentials JSON string e.g. from Parameter Store, save in the same directory as credentials.json
    Ensure that you save your credentials as exactly "credentials.json" to have them be gitignored
    """

    def __init__(self, credentials_json: str | dict):
        if type(credentials_json) == str:
            credentials_json = json.loads(credentials_json)
        self.service_readonly = self._service_from_scopes(credentials_json,
                                                          ['https://www.googleapis.com/auth/drive.metadata.readonly'])
        self.service_read_write = self._service_from_scopes(credentials_json,
                                                            ['https://www.googleapis.com/auth/drive'])
        self.output_filename = "output.json"

    @staticmethod
    def _service_from_scopes(credentials_json: str, scopes: list[str]):
        creds = service_account.Credentials.from_service_account_info(credentials_json, scopes=scopes)
        service = build('drive', 'v3', credentials=creds)
        return service

    def write_data_to_json(self, data: list | dict):
        """
        Writes JSON data to a local file
        :param data: JSON data / dict to write to file
        :param filename: Name or path of file to write output to
        """
        with open(self.output_filename, "w") as outfile:
            json.dump(data, outfile)
            print(f"Data written to {self.output_filename}")

    def get_file_or_folder(self, file_id, fields: list[str] = None) -> dict:
        """
        Gets a file or folder by File ID or Folder ID
        See: https://developers.google.com/drive/api/v3/reference/files/get
        :param file_id: ID of file/folder to fetch
        :param fields: Data about the file/folder to fetch, See https://developers.google.com/drive/api/guides/fields-parameter
        :return: File response JSON object
        """
        if fields is None:
            fields = []

        field_query = "id, name, size, createdTime, parents" + "".join([", " + field for field in fields])

        try:
            response = self.service_readonly.files().get(
                fileId=file_id,
                fields=field_query
            ).execute()
            return response
        except errors.HttpError:
            print(f"File with id {file_id} not found!")

    def query_files(self, query_lines: list[str], extra_file_fields:list[str] = None, extra_fields: list[str] = None) -> list[dict]:
        """
        Gets a list of files based on a valid query
        See https://developers.google.com/drive/api/guides/search-files for valid queries reference
        :param query_lines: A list of queries following Google's file search query syntax
        :param extra_file_fields: Extra fields in the file to include in the response
        :param extra_fields: Extra fields to include in the response
        :return: List of files
        """
        if extra_file_fields is None:
            extra_file_fields = []
        if extra_fields is None:
            extra_fields = []
        query = " and ".join(query_lines)
        print(query)
        extra_file_fields = "".join([", " + field for field in extra_file_fields])
        extra_fields = "".join([", " + field for field in extra_fields])
        fields = f"files(id, name, size, createdTime, parents{extra_file_fields}), nextPageToken" + extra_fields
        print(fields)

        all_results = []
        page_token = ""
        while True:
            response = self.service_readonly.files().list(
                q=query,
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=100,
                pageToken=page_token,
                fields=fields
            )
            try:
                result = response.execute()
                all_results += result.get("files")
                page_token = result.get('nextPageToken', None)
                if page_token is None:
                    break
            except errors.HttpError:
                # When nothing is found, response.execute() throws HttpError
                pass
        print(f"{len(all_results)} files found:")
        return all_results

    def upload(self, filename: str, mimetype: str, folder_id: str = None) -> str:
        """
        Uploads a local file to Google Drive
        See https://developers.google.com/drive/api/guides/folder#create_a_file_in_a_folder
        :param filename: Local file path to open for upload
        :param mimetype: Example: "text/csv" Consult https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
        :param folder_id: Google ID of folder to upload to. If None, will upload to the root directory.
        :return:
        """

        file_metadata = {'name': filename}
        if folder_id:
            file_metadata["parent"] = folder_id

        try:
            media = MediaFileUpload(filename, mimetype=mimetype)
            file = self.service_read_write.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(F'File ID: {file.get("id")}')
            return file.get('id')
        except errors.HttpError as error:
            print(F'An error occurred: {error}')

    def download(self, file_id: str, output_folder_name: str = "download"):
        """
        Downloads a file from Google Drive
        See https://developers.google.com/drive/api/v3/manage-downloads
        :param file_id: Google ID of file to download
        """
        request = self.service_read_write.files().get_media(fileId=file_id)
        filename = self.get_file_or_folder(file_id)["name"]
        if filename is None:
            filename = file_id
        file_media = io.FileIO(filename, mode='wb')
        downloader = MediaIoBaseDownload(file_media, request)
        done = False
        try:
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {filename} - {int(status.progress() * 100)}%")
        except errors.HttpError as error:
            print(f"An error occurred: {error}")

        os.renames(filename, f"out/{output_folder_name}/{filename}")

    def get_file_parent_folder(self, file_id: str) -> dict:
        """
        Finds the Google ID of the folder that a file is within
        :param file_id: ID of the file
        :return: Parent Folder ID
        """
        # I think you can query any attributes from here:
        # https://developers.google.com/drive/api/v2/reference/files
        response = self.get_file_or_folder(file_id, fields="parents")
        return response

    def delete_file(self, file_id: str):
        """
        Permanently delete a file, skipping the trash.
        See: https://developers.google.com/drive/api/v3/reference/files/delete
        :param file_id: Google ID of the file to delete
        """
        try:
            files = self.service_read_write.files()
            files.delete(fileId=file_id).execute()
            print(f"Deleted: {file_id}")
        except errors.HttpError as error:
            f'An error occurred deleting {file_id}:\n>>> {error}'
        except:
            raise

    def delete_matching_files_in_folder(self, folder_id, query_lines: list[str] = None, exclude_latest_n: int = 7, file_regex=None,
                                        file_size_minimum: int = 0, except_filename: str = None):
        """
        Deletes all files matching a specific query as in query_files, uses filename regex and file_size_minimum as safeguards
        :param folder_id: ID of folder to find files under
        :param query_lines: List of filters for files, see https://developers.google.com/drive/api/guides/search-files
        :param file_regex: Regex the file names must match
        :param file_size_minimum: Minimum file size
        :param except_filename: Filename to exclude from deletion
        :param exclude_latest: If True, will exclude the latest file from deletion
        """

        # Get files matching queries
        query_lines = [] if query_lines is None else query_lines
        folder_parent_query_line = f"'{folder_id}' in parents"
        if folder_parent_query_line not in query_lines:
            query_lines.append(f"'{folder_id}' in parents")

        files = self.query_files(query_lines)

        def file_created_time(file):
            return file["createdTime"]

        if exclude_latest_n > 0:
            files.sort(key=file_created_time, reverse=True)
            # latest_n_files = max(files, key=lambda x: x["createdTime"])
            latest_n_files = files[0:exclude_latest_n]
            latest_n_filenames = [file["name"] for file in latest_n_files]
            files = [file for file in files if file["name"] not in latest_n_filenames]
            print(f"Excluding latest files: {latest_n_filenames}")

        files = [file for file in files if file["name"] != except_filename or except_filename not in file["name"]]
        total_size = sum([int(file["size"]) for file in files])

        self.write_data_to_json(files)
        # Get user confirmation for deletion
        confirm(
            f"Will delete {len(files)} files in {folder_id}, example: {files[0]}, see {self.output_filename} for all files, Confirm? ")

        if file_regex is None:
            file_regex = ".+"

        # Delete all captured files
        for file in files:
            if int(file["size"]) >= file_size_minimum:
                if re.match(file_regex, file["name"]):
                    print(f"{file['name']} DELETING - ", f"{round(int(file['size']) / 10 ** 6, 2)}MB")
                    self.delete_file(file["id"])
    
        print(f"Total size: {round(total_size / 10 ** 6, 2)}MB")

    def find_all_owned_files(self, extra_file_fields: list[str] = None, extra_fields: list[str] = None) -> list[dict]:
        """
        Finds all files owned by the user and the total size of all files
        :return: List of files
        """
        if extra_file_fields is None:
            extra_file_fields = []
        if extra_fields is None:
            extra_fields = []
        query_lines = ["'me' in owners"]
        files = self.query_files(query_lines, extra_file_fields, extra_fields)
        total_size = sum([int(file["size"]) for file in files])
        print(f"Total size: {round(total_size / 10 ** 6, 2)}MB")
        return files

    def create_folder(self, folder_name: str, parent_folder_id: str = None) -> str:
        """
        Creates a folder
        :return: Google ID of the folder
        """
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        file = self.service_read_write.files().create(body=file_metadata,
                                                      fields='id').execute()
        print(f"Folder ID: {file.get('id')}")
        return file.get('id')

    def download_folder_contents(self, folder_id: str, output_folder_name: str = "download"):
        """
        Downloads all files in a folder
        :param folder_id: Google ID of the folder
        :param output_folder_name: Folder name for the download
        """
        files = self.query_files([f"'{folder_id}' in parents"])
        progress_bar = ProgressBar(len(files))
        for i, file in enumerate(files):
            if i % 10 == 0:
                progress_bar.display(i)
            self.download(file["id"], output_folder_name)
