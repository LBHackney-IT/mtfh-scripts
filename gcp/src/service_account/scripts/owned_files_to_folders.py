from aws.src.utils.progress_bar import ProgressBar
from gcp.src.service_account.utils.DriveServiceAccountClient import DriveServiceAccountClient


def owned_files_and_folders_to_tsv(
        service_account: DriveServiceAccountClient,
        extra_file_fields: list[str] = None, extra_folder_fields: list[str] = None):
    """
    This script will write two TSV files:
    - owned_files.tsv
    - folders.tsv
    """
    if extra_file_fields is None:
        extra_file_fields = []
    if extra_folder_fields is None:
        extra_folder_fields = []

    def write_to_tsv(items: list[dict], headings: list[str], filename: str = "output"):
        with open(f"out/{filename}.tsv", "w") as f:
            f.write("\t".join(headings) + "\n")
            for item in items:
                f.write("\t".join([str(item[heading]) for heading in headings]) + "\n")

    owned_files: list[dict] = service_account.find_all_owned_files(None, extra_file_fields, extra_folder_fields)

    progress_bar = ProgressBar(len(owned_files))

    folders: list[dict] = []

    for i, file in enumerate(owned_files):
        if i % 5 == 0:
            progress_bar.display(i)

        file['link'] = f"https://drive.google.com/file/d/{file['id']}"

        # -- Handle file parents --
        if not file.get("parents"):
            print(f"File {file['name']} {file['id']} has no parents")
            file['parent_id'] = ""
            file['parent_name'] = ""
            file['parent_link'] = ""
            continue

        if len(file['parents']) > 1:
            print(f"File {file['name']} {file['id']} has multiple parents")

        parent_id = file['parents'][0]

        known_parents = [folder['id'] for folder in folders]
        if parent_id not in known_parents:
            parent = service_account.get_file_or_folder(parent_id, ['permissions'])

            folders.append(parent)
        else:
            # filter list of folders to find the parent by id
            parent = next(filter(lambda folder: folder['id'] == parent_id, folders))

        file['parent_id'] = parent_id
        file['parent_name'] = parent['name']
        file['parent_link'] = f"https://drive.google.com/drive/folders/{parent_id}"

        del file['parents']

    write_to_tsv(owned_files, list(owned_files[0].keys()), "owned_files")

    def perms_string_for_role(permissions: list[dict], role: str) -> str:
        emails = [perm.get('emailAddress') for perm in permissions if
                  perm.get('role') == role and not perm.get('deleted')]
        if emails:
            return ", ".join([email for email in emails if email])
        else:
            return ""

    for folder in folders:
        perms = folder.get('permissions', [])
        folder['link'] = f"https://drive.google.com/drive/folders/{folder['id']}"

        folder['writers'] = perms_string_for_role(perms, 'writer')
        folder['readers'] = perms_string_for_role(perms, 'reader')
        folder['owners'] = perms_string_for_role(perms, 'owner')
        del folder['permissions']

    folders_headings = list(folders[0].keys())
    write_to_tsv(folders, folders_headings, "folders")
