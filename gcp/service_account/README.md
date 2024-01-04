# Google Account Client

This tool is designed to assist with controlling a Google service account in Google Drive.

## Setup

Ensure you have configured an AWS profile to access the parameter store with the service account's credentials. Refer to
the enum in `aws/src/enums` for profile names.

The parameter store path for getting the service account credentials is configured in `get_credentials.py`.

## Use

1) Open `main.py`
2) Use the `service_account` instance's methods inside the `main()` function
3) Uncomment the import from `main.py` in the `use_case.py` file in the root directory of the scripts repo and call the imported function
4) Run `use_case.py` to execute the use case

## Examples

Get CSV files in a folder with a specified ID which aren't in the trash and have a name containing "
2022", then write these to a json file

```python
def main():
    folder_id = input("Enter folder ID: ")
    query_lines = [
        f"'{folder_id}' in parents",
        "trashed=false",
        "mimeType = 'text/csv'",
        f"name contains '2022'",
    ]

    files = service_account.query_files(query_lines)
    service_account.write_data_to_json(files)
```

___

Uploads a CSV file in the current directory named "test.csv" to the same parent folder of a file specified by ID

You could also specify the path to the file under the `filename` like `./files_to_upload/test.csv`

```python
if __name__ == "__main__":
    file_id = input("Enter file ID: ")
    parent_directory = service_account.get_file_parent_folder(file_id)
    service_account.upload(filename="test.csv", mimetype="text/csv", folder_id=parent_directory)
```

___

Deletes all files in a folder specified by ID which match the regex (Start with
BadFile, have csv in name) and which are greater than 1000000 bytes (1GB) in size

This will ask for user confirmation

```python
if __name__ == "__main__":
    folder_id = input("Enter folder ID: ")
    delete_matching_files_in_folder(folder_id, file_regex="^BadFile.*csv", file_size_minimum=1000000)
```
