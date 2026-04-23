import io
import json
import os
import re
from typing import Any

from google.oauth2 import service_account
from googleapiclient import errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from aws.utils.progress_bar import ProgressBar
from utils.confirm import confirm


class DriveServiceAccountClient:
    # pylint: disable=no-member  # google-api-python-client builds Resource methods dynamically
    def __init__(self, credentials_json: str | dict):
        if isinstance(credentials_json, str):
            credentials_json = json.loads(credentials_json)
        assert isinstance(credentials_json, dict), "credentials_json must be a dict"
        self.service_readonly: Any = self._service_from_scopes(
            credentials_json,
            ["https://www.googleapis.com/auth/drive.metadata.readonly"],
        )
        self.service_read_write: Any = self._service_from_scopes(
            credentials_json, ["https://www.googleapis.com/auth/drive"]
        )
        self.output_filename = "output.json"

    @staticmethod
    def _service_from_scopes(credentials_json: dict, scopes: list[str]) -> Any:
        creds = service_account.Credentials.from_service_account_info(
            credentials_json, scopes=scopes
        )
        service = build("drive", "v3", credentials=creds)
        return service

    def get_file_or_folder(
        self, file_id, fields: list[str] | None = None
    ) -> dict | None:
        """
        Gets a file or folder by File ID or Folder ID
        See: https://developers.google.com/drive/api/v3/reference/files/get
        :param file_id: ID of file/folder to fetch
        :param fields: Data about the file/folder to fetch, See https://developers.google.com/drive/api/guides/fields-parameter
        :return: File response JSON object
        """
        if fields is None:
            fields = []

        field_query = "id, name, size, createdTime, parents" + "".join(
            [", " + field for field in fields]
        )

        try:
            response = (
                self.service_readonly.files()
                .get(fileId=file_id, fields=field_query)
                .execute()
            )
            return response
        except errors.HttpError:
            print(f"File with id {file_id} not found!")
            return None

    def query_files(
        self,
        query_lines: list[str],
        extra_file_fields: list[str] | None = None,
        extra_fields: list[str] | None = None,
    ) -> list[dict]:
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
        extra_file_fields_str = "".join([", " + field for field in extra_file_fields])
        extra_fields_str = "".join([", " + field for field in extra_fields])
        fields = (
            f"files(id, name, size, createdTime, parents{extra_file_fields_str}), nextPageToken"
            + extra_fields_str
        )
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
                fields=fields,
            )
            try:
                result = response.execute()
                all_results += result.get("files")
                page_token = result.get("nextPageToken")
                if page_token is None:
                    break
            except errors.HttpError:
                # When nothing is found, response.execute() throws HttpError
                pass
        print(f"{len(all_results)} files found:")
        return all_results

    def upload(self, filename: str, mimetype: str, folder_id: str) -> str | None:
        """
        Uploads a local file to Google Drive
        See https://developers.google.com/drive/api/guides/folder#create_a_file_in_a_folder
        :param filename: Local file path to open for upload
        :param mimetype: Example: "text/csv" Consult https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
        :param folder_id: Google ID of folder to upload to.
        :return:
        """
        file_metadata = {"name": filename, "parent": folder_id}

        try:
            media = MediaFileUpload(filename, mimetype=mimetype)
            file = (
                self.service_read_write.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            print(f'File ID: {file.get("id")}')
            return file.get("id")
        except errors.HttpError as error:
            print(f"An error occurred: {error}")

    def download(self, file_id: str):
        """
        Downloads a file from Google Drive
        See https://developers.google.com/drive/api/v3/manage-downloads
        :param file_id: Google ID of file to download
        """
        request = self.service_read_write.files().get_media(fileId=file_id)
        file_info = self.get_file_or_folder(file_id)
        filename = file_info["name"] if file_info else file_id
        file_media = io.FileIO(filename, mode="wb")
        downloader = MediaIoBaseDownload(file_media, request)
        done = False
        try:
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {filename} - {int(status.progress() * 100)}%")
        except errors.HttpError as error:
            print(f"An error occurred: {error}")

        os.renames(filename, f"out/download/{filename}")

    def download_folder_contents(self, folder_id: str):
        """
        Downloads all files in a folder
        :param folder_id: Google ID of the folder
        """
        files = self.query_files([f"'{folder_id}' in parents"])
        progress_bar = ProgressBar(len(files))
        for i, file in enumerate(files):
            if i % 10 == 0:
                progress_bar.display(i)
            print(f"Downloading {file['name']}")
            self.download(file["id"])

    def copy_file(
        self, file_id: str, new_name: str, parent_folder_id: str
    ) -> str | None:
        """
        Copies a file into the same or different folder with a new name.
        See: https://developers.google.com/drive/api/v3/reference/files/copy
        :param file_id: Google ID of the file to copy
        :param new_name: Name for the copied file
        :param parent_folder_id: Google ID of the destination folder
        :return: File ID of the new copy, or None on error
        """
        try:
            result = (
                self.service_read_write.files()
                .copy(
                    fileId=file_id,
                    body={"name": new_name, "parents": [parent_folder_id]},
                    supportsAllDrives=True,
                )
                .execute()
            )
            print(f"Copied to: {result.get('id')} ({new_name})")
            return result.get("id")
        except errors.HttpError as error:
            print(f"An error occurred copying {file_id}:\n>>> {error}")
            return None

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
            print(f"An error occurred deleting {file_id}:\n>>> {error}")

    def delete_matching_files_in_folder(
        self,
        folder_id,
        query_lines: list[str] | None = None,
        exclude_latest_n: int = 7,
        file_regex: str | None = None,
        file_size_minimum: int = 0,
    ):
        """
        Deletes all files in a folder matching a specific query as in query_files, with related safeguards
        :param folder_id: ID of folder to find files under
        :param query_lines: List of filters for files, see https://developers.google.com/drive/api/guides/search-files
        :param exclude_latest_n: Will exclude the latest {number} files from deletion, default 7
        :param file_regex: Regex the file names must match
        :param file_size_minimum: Minimum file size
        """

        # Get files matching queries
        query_lines = [] if query_lines is None else query_lines
        folder_parent_query_line = f"'{folder_id}' in parents"
        if folder_parent_query_line not in query_lines:
            query_lines.append(f"'{folder_id}' in parents")

        files = self.query_files(query_lines)

        if exclude_latest_n > 0:
            files.sort(key=lambda file: file["createdTime"], reverse=True)
            latest_n_files = files[0:exclude_latest_n]
            latest_n_filenames = [file["name"] for file in latest_n_files]
            files = [file for file in files if file["name"] not in latest_n_filenames]
            print(f"Excluding latest files: {latest_n_filenames}")

        total_size = sum([int(file["size"]) for file in files])

        if file_regex is None:
            file_regex = ".+"

        if file_size_minimum > 0:
            files = [file for file in files if int(file["size"]) >= file_size_minimum]

        files = [file for file in files if re.match(file_regex, file["name"])]

        with open(self.output_filename, "w") as outfile:
            json.dump(files, outfile)
            print(f"Data written to {self.output_filename}")

        # Get user confirmation for deletion
        folder_info = self.get_file_or_folder(folder_id)
        folder_name = folder_info["name"] if folder_info else folder_id
        print(f"Total size: {round(total_size / 10 ** 6, 2)}MB")
        confirm(
            f"Will delete {len(files)} files in {folder_name} ({folder_id}), "
            f"example: {files[0]}, see {self.output_filename} for all files, Confirm? "
        )

        # Delete all captured files
        for file in files:
            print(
                f"{file['name']} DELETING - ",
                f"{round(int(file['size']) / 10 ** 6, 2)}MB",
            )
            self.delete_file(file["id"])

    def find_all_owned_files(
        self,
        extra_query_lines: list[str] | None = None,
        extra_file_fields: list[str] | None = None,
        extra_fields: list[str] | None = None,
    ) -> list[dict]:
        """
        Finds all files owned by the user and the total size of all files
        :extra_query_lines: Extra query lines to filter files by, see query_files method
        :extra_file_fields: Extra fields to include in the file response, see query_files method
        :return: List of files
        """
        if extra_query_lines is None:
            extra_query_lines = []
        if extra_file_fields is None:
            extra_file_fields = []
        if extra_fields is None:
            extra_fields = []
        query_lines = ["'me' in owners"] + extra_query_lines
        files = self.query_files(query_lines, extra_file_fields, extra_fields)
        total_size = sum([int(file["size"]) for file in files])
        print(f"Total size: {round(total_size / 10 ** 6, 2)}MB")
        return files
