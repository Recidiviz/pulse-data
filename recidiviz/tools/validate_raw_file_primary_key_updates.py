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
# ============================================================================
"""Tool to validate raw file primary key updates.

This script is intended to be run in CI whenever a file in recidiviz/ingest/direct/regions/*/raw_data/*.yaml
is updated. If there is an update to a file's primary keys in the --commit-range passed to
the script, and none of the commits in the commit range are prefixed with [ALLOW_PK_UPDATES],
the script will fail. If a new raw data yaml file is added or deleted, this is allowed and will not
cause the script to fail.

Files for states where automatic raw data pruning is not enabled are skipped from validation.

Example usage:
$ python -m recidiviz.tools.validate_raw_file_primary_key_updates [--commit-range RANGE]
"""
import argparse
import logging
import re
import sys
from typing import FrozenSet, Optional, Set

import yaml

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import (
    automatic_raw_data_pruning_enabled_for_state_and_instance,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.validation.git_validation_utils import (
    get_commits_with_pattern,
    get_file_content_at_commit,
    get_modified_files,
    parse_commit_range,
)

RAW_DATA_YAML_PATTERN = re.compile(
    r"recidiviz/ingest/direct/regions/([^/]+)/raw_data/.*\.yaml$"
)
ALLOW_PRIMARY_KEY_UPDATES_TAG = "[ALLOW_PK_UPDATES]"
PRIMARY_KEYS_YAML_KEY = "primary_key_cols"


def _parse_raw_data_file_path(file_path: str) -> Optional[str]:
    """Extracts region code from a raw data YAML file path.

    Args:
        file_path: Path like 'recidiviz/ingest/direct/regions/us_ca/raw_data/test_file.yaml'

    Returns:
        Region code or None if path doesn't match expected pattern.
    """
    match = RAW_DATA_YAML_PATTERN.match(file_path)
    if not match:
        return None

    return match.group(1)


def _is_file_exempt_from_validation(file_path: str) -> bool:
    """Checks if a raw data file is exempt from primary key validation.

    Files are exempt if automatic raw data pruning is not enabled for the state.

    Args:
        file_path: Path to the raw data YAML file

    Returns:
        True if the file is exempt from validation, False otherwise.
    """
    region_code = _parse_raw_data_file_path(file_path)
    if not region_code:
        logging.warning("Could not parse region code from path: %s", file_path)
        return False

    return not any(
        automatic_raw_data_pruning_enabled_for_state_and_instance(
            StateCode(region_code.upper()), raw_data_instance
        )
        for raw_data_instance in DirectIngestInstance
    )


def _get_modified_raw_data_files(commit_range: str) -> FrozenSet[str]:
    """Returns a set of all raw data YAML files that have been modified in the given commit range,
    excluding files for states where automatic raw data pruning is not enabled."""
    all_modified_files = get_modified_files(commit_range)

    raw_data_files = [
        file for file in all_modified_files if RAW_DATA_YAML_PATTERN.match(file)
    ]

    non_exempt_files = []
    for file_path in raw_data_files:
        if _is_file_exempt_from_validation(file_path):
            logging.info("Skipping validation for exempt file: %s", file_path)
        else:
            non_exempt_files.append(file_path)

    return frozenset(non_exempt_files)


def _extract_primary_keys_from_yaml(yaml_content: str) -> Set[str]:
    """Extracts primary key columns from YAML content."""
    data = yaml.safe_load(yaml_content)
    if PRIMARY_KEYS_YAML_KEY not in data:
        return set()

    primary_keys = data[PRIMARY_KEYS_YAML_KEY]
    if primary_keys is None:
        return set()

    return set(primary_keys)


def _check_primary_key_updates(file_path: str, commit_range: str) -> bool:
    """Checks if a file has primary key changes in the given commit range."""
    base_commit, head_commit = parse_commit_range(commit_range)

    base_content = get_file_content_at_commit(file_path, base_commit)
    head_content = get_file_content_at_commit(file_path, head_commit)
    if not base_content or not head_content:
        # If the file didn't exist at either commit, we can't have a change
        return False

    base_primary_keys = _extract_primary_keys_from_yaml(base_content)
    head_primary_keys = _extract_primary_keys_from_yaml(head_content)

    return base_primary_keys != head_primary_keys


def validate_raw_file_primary_key_updates(commit_range: str) -> bool:
    """Main function to validate raw file primary key updates. Returns True if validation fails
    (i.e., primary key updates detected without [ALLOW_PK_UPDATES] commit tag)."""
    modified_files = _get_modified_raw_data_files(commit_range)

    if not modified_files:
        logging.info(
            "No raw data yaml files modified in commit range [%s]", commit_range
        )
        return False

    files_with_pk_changes = []
    for file_path in modified_files:
        if _check_primary_key_updates(file_path, commit_range):
            files_with_pk_changes.append(file_path)

    if not files_with_pk_changes:
        logging.info("No primary key updatees detected in modified raw data files")
        return False

    if get_commits_with_pattern(commit_range, ALLOW_PRIMARY_KEY_UPDATES_TAG):
        logging.info(
            "Primary key updates detected but '%s' found in commit messages. Allowing changes.",
            ALLOW_PRIMARY_KEY_UPDATES_TAG,
        )
        return False

    logging.error("Primary key updates detected in the following files:")
    for file_path in files_with_pk_changes:
        logging.error("  - %s", file_path)
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--commit-range",
        help="The git commit range to compare against.",
        required=True,
    )
    args = parser.parse_args()

    failed = validate_raw_file_primary_key_updates(args.commit_range)
    if failed:
        logging.error(
            "Primary keys may only be updated for files with <= 1 non-invalidated file currently imported."
            "To confirm that this is the case, prefix your commit message with '%s' to bypass this check.",
            ALLOW_PRIMARY_KEY_UPDATES_TAG,
        )
        sys.exit(1)
