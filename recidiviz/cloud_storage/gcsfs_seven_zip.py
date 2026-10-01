# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Extraction of 7z archives stored in GCS.

Lives apart from gcs_file_system_impl so that py7zr, which is not installed in the
Cloud Composer environment, stays out of the import graph that the Airflow DAGs load.
Only the cloud function that decrypts 7z ingest files imports this module.
"""
import io
import logging
import lzma
import os
import tempfile

import py7zr
import py7zr.exceptions

from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle


def _extract_to_local_dir(
    downloaded_bytes: io.BytesIO,
    seven_zip_file_path: GcsfsFilePath,
    local_dir: str,
    *,
    password: str | None,
) -> list[str]:
    """Extracts the downloaded 7z archive into |local_dir|. Decrypts the archive with
    |password| when one is given; pass None for an unencrypted archive. Returns the
    names of the files written.

    Raises a ValueError if the file is not a 7z archive, is encrypted but |password| is
    None, or the |password| is incorrect.
    """
    if not py7zr.is_7zfile(downloaded_bytes):
        raise ValueError(
            f"Path [{seven_zip_file_path.uri()}] is not a 7z file. Cannot extract."
        )

    try:
        with py7zr.SevenZipFile(
            downloaded_bytes, mode="r", password=password
        ) as archive:
            extracted_names = archive.getnames()
            archive.extractall(path=local_dir)
    except py7zr.exceptions.PasswordRequired as e:
        raise ValueError(
            f"7z file [{seven_zip_file_path.uri()}] is encrypted but no password "
            "was provided."
        ) from e
    except (
        py7zr.exceptions.Bad7zFile,
        py7zr.exceptions.CrcError,
        py7zr.exceptions.DecompressionError,
        lzma.LZMAError,
    ) as e:
        # An incorrect password does not raise a distinct exception: the wrong key
        # decrypts to garbage that fails either its CRC check or LZMA decompression.
        # A genuinely corrupt archive fails the same way, so we cannot tell the two
        # apart here.
        raise ValueError(
            f"Could not extract 7z file [{seven_zip_file_path.uri()}]: it is "
            f"corrupt or the decryption password is incorrect."
        ) from e

    return extracted_names


def un7z(
    fs: GCSFileSystem,
    seven_zip_file_path: GcsfsFilePath,
    destination_dir: GcsfsDirectoryPath,
    *,
    password: str | None,
) -> list[GcsfsFilePath]:
    """Uses the provided fs to extract the 7z archive at the provided
    |seven_zip_file_path| and write all internal files into the provided
    |destination_dir|. Decrypts the archive with |password| when one is given; pass None
    for an unencrypted archive. Returns the list of extracted paths generated.

    Raises a ValueError if the file is not a 7z archive, is encrypted but |password| is
    None, or the |password| is incorrect.
    """
    logging.info("Downloading 7z file [%s] as bytes", seven_zip_file_path.uri())
    downloaded_bytes = io.BytesIO(fs.download_as_bytes(seven_zip_file_path))
    logging.info(
        "Finished downloading 7z file [%s] as bytes", seven_zip_file_path.uri()
    )

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        extracted_names = _extract_to_local_dir(
            downloaded_bytes, seven_zip_file_path, temp_dir, password=password
        )

        new_paths = []
        for extracted_name in extracted_names:
            local_path = os.path.join(temp_dir, extracted_name)
            if not os.path.isfile(local_path):
                continue
            new_path = GcsfsFilePath.from_directory_and_file_name(
                destination_dir, extracted_name
            )
            logging.info(
                "Uploading extracted file %s to %s", extracted_name, new_path.uri()
            )
            fs.upload_from_contents_handle_stream(
                path=new_path,
                contents_handle=LocalFileContentsHandle(local_path, cleanup_file=False),
                content_type=BYTES_CONTENT_TYPE,
            )
            new_paths.append(new_path)

    logging.info("Finished uploading extracted files")
    return new_paths
