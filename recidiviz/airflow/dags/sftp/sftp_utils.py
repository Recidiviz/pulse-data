# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Sftp utility functions"""
import datetime
import re

_FILE_NAME_REGEX = re.compile(
    r"^(?P<file_tag>[_A-Za-z\d]*?)"  # file_tag
    r"(?:-(?P<filename_suffix>\d+))?"  # Optional filename_suffix
    r"\.(?P<extension>[A-Za-z]+)$"  # Extension
)


def utc_update_datetime_from_post_processed_normalized_file_path(
    post_processed_normalized_file_path: str,
) -> datetime.datetime:
    """Extracts the utc update datetime from a post-processed normalized file path.

    Args:
        post_processed_normalized_file_path: The post-processed normalized file path
            in the format "{update_datetime}/{optional_subpath}/{file_tag}{optional_suffix}.{ext}".
            We expect the update_datetime to be in utc but without a timezone designator.
    Returns:
        The extracted utc update datetime.
    """
    datetime_str = post_processed_normalized_file_path.split("/")[0]
    return datetime.datetime.fromisoformat(datetime_str).replace(
        tzinfo=datetime.timezone.utc
    )


def file_tag_from_post_processed_normalized_file_path(
    post_processed_normalized_file_path: str,
) -> str:
    """Extracts the file tag from a post-processed normalized file path.

    Args:
        post_processed_normalized_file_path: The post-processed normalized file path
            in the format "{update_datetime}/{optional_subpath}/{file_tag}{optional_suffix}.{ext}".
    Returns:
        The extracted file tag.
    """
    file_name = post_processed_normalized_file_path.split("/")[-1]

    match = _FILE_NAME_REGEX.match(file_name)
    if not match:
        raise ValueError(
            f"Could not parse file tag from [{post_processed_normalized_file_path}]"
        )
    return match.group("file_tag")
