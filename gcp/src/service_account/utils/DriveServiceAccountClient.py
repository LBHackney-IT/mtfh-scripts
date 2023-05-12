import json
import re

from google.oauth2 import service_account
from googleapiclient import errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


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

    def write_data_to_json(self, data: dict):
        """
        Writes JSON data to a local file
        :param data: JSON data / dict to write to file
        :param filename: Name or path of file to write output to
        """
        with open(self.output_filename, "w") as outfile:
            json.dump(data, outfile)
            print(f"Data written to {self.output_filename}")

    def get_file_or_folder(self, file_id, fields=None) -> dict:
        """
        Gets a file or folder by File ID or Folder ID
        See: https://developers.google.com/drive/api/v3/reference/files/get
        :param file_id: ID of file/folder to fetch
        :param fields: Data about the file/folder to fetch, See https://developers.google.com/drive/api/guides/fields-parameter
        :return: File response JSON object
        """
        try:
            response = self.service_readonly.files().get(
                fileId=file_id, fields=fields
            ).execute()
            return response
        except errors.HttpError:
            print(f"File with id {file_id} not found!")

    def query_files(self, query_lines: list[str]) -> list[str]:
        """
        Gets a list of files based on a valid query
        See https://developers.google.com/drive/api/guides/search-files for valid queries reference
        :param query_lines: A list of queries following Google's file search query syntax
        :return: List of files
        """
        query = " and ".join(query_lines)
        print(query)
        all_results = []
        page_token = None
        while True:
            response = self.service_readonly.files().list(
                q=query,
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token,
                fields="files(id, name, size, createdTime)"
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

        print(all_results)
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
            return

    def get_file_parent_folder(self, file_id: str) -> str:
        """
        Finds the Google ID of the folder that a file is within
        :param file_id: ID of the file
        :return: Parent Folder ID
        """
        # I think you can query any attributes from here:
        # https://developers.google.com/drive/api/v2/reference/files
        response = self.get_file_or_folder(file_id, fields="parents")
        return response

    def delete_file(self, file_obj: dict):
        """
        Permanently delete a file, skipping the trash.
        See: https://developers.google.com/drive/api/v3/reference/files/delete
        :param file_id: Google ID of the file to delete
        """
        file_id = str(file_obj["id"])
        try:

            files = self.service_read_write.files()
            files.delete(fileId=file_id).execute()
            print(f"Deleted: {file_id}")
        except errors.HttpError as error:
            f'An error occurred deleting {file_id}:\n>>> {error}'

    def delete_matching_files_in_folder(self, folder_id, query_lines: list[str] = None, file_regex=None,
                                        file_size_minimum: int = 0, except_filename: str = None, exclude_latest=False):
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

        if exclude_latest:
            latest_file = max(files, key=lambda x: x["createdTime"])
            files = [file for file in files if file["name"] != latest_file["name"]]
            print(f"Excluding latest file: {latest_file['name']}")

        files = [file for file in files if file["name"] != except_filename or except_filename not in file["name"]]

        self.write_data_to_json(files)
        # Get user confirmation for deletion
        confirmation = input(
            f"Will delete {len(files)} files in {folder_id}, example: {files[0]}, see {self.output_filename} for all files\nContinue? Y/Yes or N/No: ")
        if confirmation.lower() not in ["y", "yes"]:
            print("Aborting")
            return

        if file_regex is None:
            file_regex = ".+"

        # latest_file = max(files, key=lambda x: x["createdTime"])
        #
        # files = [file for file in files if file["name"] != latest_file["name"]]

        # Delete all captured files
        for file in files:
            if int(file["size"]) >= file_size_minimum:
                if file_regex is not None:
                    if re.match(file_regex, file["name"]):
                        print(f"{file['name']} DELETING - ", f"{round(int(file['size']) / 10 ** 6, 2)}MB")
                        self.delete_file(file)
                else:
                    print(f"{file['name']} DELETING - ", f"{round(int(file['size']) / 10 ** 6, 2)}MB")
                    self.delete_file(file)
