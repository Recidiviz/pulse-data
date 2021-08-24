# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Downloads the data and manifest files from Google Drive and writes them to a local directory to be tested.

This includes exporting the Google Sheets as CSVs and downloading the manifests. The first time this is run, you must
download credentials.json from here: https://console.developers.google.com/apis/credentials. These are long-lasting
credentials so put them in a permanent directory. Pass the path to that directory to the command as
`--credentials-directory`.

Example Usage:
python -m recidiviz.tools.justice_counts.google_drive --state US_MS --system CORRECTIONS \
    --drive-folder-id abc123 --base-directory ~/jc-data --credentials-directory ~/credentials
"""
import argparse
import enum
import io
import logging
import os
import pickle
from typing import List, Optional

import attr
import gspread
import pandas as pd
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.http import MediaIoBaseDownload

from recidiviz.common.constants import states
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts.manual_upload import csv_filename

# If modifying these scopes, delete the file token.pickle from your credentials directory.
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_credentials(directory: str, readonly: bool) -> Credentials:
    creds = None

    scopes = SCOPES
    if readonly:
        scopes = [f"{scope}.readonly" for scope in SCOPES]

    token_path = os.path.join(directory, "token.pickle")
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        with open(token_path, "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(directory, "credentials.json"), scopes
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "wb") as token:
            pickle.dump(creds, token)

    return creds


def get_drive_service(creds: Credentials) -> Resource:
    return build("drive", "v3", credentials=creds)


class FileType(enum.Enum):
    # YAML is just a binary file
    BINARY = "application/octet-stream"
    CSV = "text/csv"
    DOC = "application/vnd.google-apps.document"
    FOLDER = "application/vnd.google-apps.folder"
    PLAINTEXT = "text/plain"
    SHEET = "application/vnd.google-apps.spreadsheet"
    SHEET_EXCEL = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@attr.s
class DriveItem:
    id: str = attr.ib()
    name: str = attr.ib()
    file_type: FileType = attr.ib(converter=FileType)


class Drive:
    """Utility class for working with the Drive and Sheets clients."""

    drive: Resource
    gc: gspread.Client

    def __init__(self, credentials_directory: str, readonly: bool = True) -> None:
        creds = get_credentials(credentials_directory, readonly)
        self.drive = get_drive_service(creds)
        self.gc = gspread.authorize(creds)

    def get_items(
        self, parent_id: str, file_type: Optional[FileType], name: Optional[str] = None
    ) -> List[DriveItem]:
        """Returns all of the matching files in Google Drive."""
        query_string = f'"{parent_id}" in parents'
        if file_type is not None:
            query_string += f' and mimeType="{file_type.value}"'
        if name is not None:
            query_string += f' and name="{name}"'

        results: List[DriveItem] = []
        page_token: Optional[str] = None
        while not results or page_token is not None:
            logging.info("get %s", query_string)
            response = (
                self.drive.files()
                .list(
                    q=query_string,
                    pageSize=10,
                    fields="nextPageToken, files(id, name, mimeType)",
                    spaces="drive",
                    pageToken=page_token,
                )
                .execute()
            )
            items = response.get("files", [])

            if not items:
                break

            results.extend(
                DriveItem(id=item["id"], name=item["name"], file_type=item["mimeType"])
                for item in items
            )
            page_token = response.get("nextPageToken")

        return results

    def get_folder(self, name: str, parent_id: str) -> DriveItem:
        items = self.get_items(parent_id, FileType.FOLDER, name)
        if len(items) > 1:
            raise ValueError(f"Expected only one directory with name: {name}")
        return items[0]

    def upload_csv(self, sheet_item: DriveItem, csv_path: str, sheet_name: str) -> None:
        """Uploads data from the csv into the given sheet.

        Note this will remove all other worksheets and replace the first worksheet in
        the given sheet.
        """
        with open(csv_path, encoding="utf-8") as csv_data:
            self.gc.import_csv(sheet_item.id, csv_data.read())
        sheet = self.gc.open_by_key(sheet_item.id)
        [worksheet] = sheet.worksheets()
        worksheet.update_title(sheet_name)

    def download_sheet(self, sheet_item: DriveItem, local_directory: str) -> None:
        sheet = self.gc.open_by_key(sheet_item.id)
        logging.info("Exporting sheet '%s'", sheet.title)

        for worksheet in sheet.worksheets():
            # pylint: disable=protected-access
            if worksheet._properties.get("hidden"):  # type: ignore[attr-defined]
                logging.info("Skipping hidden worksheet '%s'", worksheet.title)
                continue
            csv_name = csv_filename(sheet.title, worksheet.title)
            logging.info(
                "Downloading worksheet '%s' to '%s'", worksheet.title, csv_name
            )
            df = pd.DataFrame(worksheet.get_all_records())
            df.to_csv(os.path.join(local_directory, csv_name), index=False)

    def download_sheets(self, parent_id: str, local_directory: str) -> None:
        items = self.get_items(parent_id, FileType.SHEET)

        if not items:
            raise ValueError(f"Expected sheets in folder: {parent_id}")

        for item in items:
            self.download_sheet(item, local_directory)

    def download_file(self, item: DriveItem, local_directory: str) -> None:
        logging.info("Exporting file '%s'", item.name)
        request = self.drive.files().get_media(fileId=item.id)
        fh = io.FileIO(os.path.join(local_directory, item.name), mode="wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logging.info(". Download %d%%.", int(status.progress() * 100))

    def download_manifests(self, parent_id: str, local_directory: str) -> None:
        items = self.get_items(parent_id, None)
        for item in items:
            if item.name.endswith(".yaml"):
                # If a YAML is ever opened with Google Docs, it will create a separate DOC file. This can't be
                # downloaded directly so we skip it.
                if item.file_type is FileType.DOC:
                    logging.info("Skipping %r", item)
                else:
                    self.download_file(item, local_directory)

    def download_data(self, parent_id: str, local_directory: str) -> None:
        self.download_manifests(parent_id, local_directory=local_directory)
        self.download_sheets(parent_id, local_directory=local_directory)

        # Recursively download files from subfolders.
        for folder in self.get_items(parent_id, FileType.FOLDER):
            self.download_data(
                folder.id, local_directory=os.path.join(local_directory, folder.name)
            )


def get_data_folder(
    drive: Drive,
    state_code: states.StateCode,
    system: schema.System,
    base_drive_folder_id: str,
) -> DriveItem:
    state_folder = drive.get_folder(state_code.get_state().name, base_drive_folder_id)
    corrections_folder = drive.get_folder(system.value.title(), state_folder.id)
    return drive.get_folder("Data", corrections_folder.id)


def download_data(
    state_code: states.StateCode,
    system: schema.System,
    base_drive_folder_id: str,
    base_local_directory: str,
    credentials_directory: str,
) -> None:
    local_directory = os.path.join(base_local_directory, state_code.value, system.value)
    os.makedirs(local_directory, exist_ok=True)

    # Clear the directory
    for file in os.listdir(local_directory):
        os.remove(os.path.join(local_directory, file))

    drive = Drive(credentials_directory)

    data_folder = get_data_folder(drive, state_code, system, base_drive_folder_id)
    drive.download_data(data_folder.id, local_directory=local_directory)


def _create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--state",
        required=True,
        choices=[state.value for state in states.StateCode],
        help="The state to download manual data for.",
    )
    parser.add_argument(
        "--system",
        required=True,
        choices=[system.value for system in schema.System],
    )
    parser.add_argument(
        "--base-directory",
        required=True,
        type=str,
        help="The base local directory to write downloaded files to.",
    )
    parser.add_argument(
        "--drive-folder-id",
        required=True,
        type=str,
        help="The id for the folder root Justice Counts Data Collection folder, which contains subdirectories with all "
        "of the states. The id is the last part of the url, e.g. 'abc123' from "
        "'https://drive.google.com/drive/folders/abc123'.",
    )
    parser.add_argument(
        "--credentials-directory",
        required=False,
        default=".",
        type=str,
        help="Directory where the 'credentials.json' live, as well as the cached token.",
    )
    parser.add_argument(
        "--log",
        required=False,
        default="INFO",
        type=logging.getLevelName,
        help="Set the logging level",
    )
    return parser


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)
    download_data(
        states.StateCode(arguments.state),
        schema.System(arguments.system),
        arguments.drive_folder_id,
        arguments.base_directory,
        arguments.credentials_directory,
    )
