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
"""Mixin for SFTP download delegates to verify required files are present before uploading
to ingest bucket."""
import logging
from collections import defaultdict

from recidiviz.airflow.dags.sftp.sftp_utils import (
    file_tag_from_post_processed_normalized_file_path,
    utc_update_datetime_from_post_processed_normalized_file_path,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_config_enums import (
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.raw_file_references_utils import (
    get_all_referenced_file_tags,
)
from recidiviz.ingest.direct.raw_data.state_raw_file_chunking_metadata_factory import (
    StateRawFileChunkingMetadataFactory,
)


class RequireFilesBeforeUploadMixin:
    """Mixin that provides methods to verify required files are present before uploading
    to ingest bucket."""

    @staticmethod
    def _are_all_required_file_chunks_present(
        region_code: str,
        file_tag_to_paths: dict[str, set[str]],
        required_file_tags: set[str],
    ) -> bool:
        """Returns True if all chunks for chunked files in |required_file_tags| are present."""
        raw_file_chunking_metadata = StateRawFileChunkingMetadataFactory.build(
            region_code=region_code
        )
        chunked_file_tag_to_expected_count = {
            file_tag: raw_file_chunking_metadata.get_current_expected_file_count(
                file_tag
            )
            for file_tag in required_file_tags
            if (
                c := raw_file_chunking_metadata.get_current_expected_file_count(
                    file_tag
                )
            )
            and c > 1
        }
        file_tags_with_missing_chunks = {
            file_tag
            for file_tag, expected_chunk_count in chunked_file_tag_to_expected_count.items()
            if len(file_tag_to_paths.get(file_tag, set())) != expected_chunk_count
        }
        if not file_tags_with_missing_chunks:
            return True

        for file_tag in sorted(file_tags_with_missing_chunks):
            actual_chunk_count = len(file_tag_to_paths.get(file_tag, set()))
            logging.info(
                "File tag [%s] has [%s] chunks present, expected [%s]",
                file_tag,
                actual_chunk_count,
                chunked_file_tag_to_expected_count[file_tag],
            )

        return False

    @staticmethod
    def _are_all_required_file_tags_present(
        file_tag_to_paths: dict[str, set[str]],
        required_file_tags: set[str],
    ) -> bool:
        """Returns True if all |required_file_tags| are present in |file_tag_to_paths|."""
        missing_file_tags = required_file_tags - file_tag_to_paths.keys()
        if not missing_file_tags:
            return True

        logging.info(
            "Missing required file tags for upload: %s",
            ", ".join(sorted(missing_file_tags)),
        )

        return False

    @classmethod
    def _are_all_required_files_present(
        cls,
        region_code: str,
        ingest_ready_normalized_file_paths: list[str],
        required_file_tags: set[str],
    ) -> bool:
        """Returns True if all |required_file_tags| are present in the list of
        |ingest_ready_normalized_file_paths| for the most recent date."""
        if not ingest_ready_normalized_file_paths:
            raise ValueError("ingest_ready_normalized_file_paths cannot be empty")

        files_by_date = defaultdict(set)
        for normalized_file in ingest_ready_normalized_file_paths:
            update_date = utc_update_datetime_from_post_processed_normalized_file_path(
                normalized_file
            ).date()
            files_by_date[update_date].add(normalized_file)

        most_recent_date = max(files_by_date)
        logging.info(
            "Checking required files for upload on most recent date: [%s]",
            most_recent_date,
        )

        file_tag_to_paths = defaultdict(set)
        for normalized_file in files_by_date[most_recent_date]:
            file_tag_to_paths[
                file_tag_from_post_processed_normalized_file_path(normalized_file)
            ].add(normalized_file)

        return cls._are_all_required_file_tags_present(
            file_tag_to_paths=file_tag_to_paths,
            required_file_tags=required_file_tags,
        ) and cls._are_all_required_file_chunks_present(
            region_code=region_code,
            file_tag_to_paths=file_tag_to_paths,
            required_file_tags=required_file_tags,
        )

    @classmethod
    def are_all_referenced_daily_files_present(
        cls,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        ingest_ready_normalized_file_paths: list[str],
    ) -> bool:
        """Returns True if all file tags meeting the following criteria are present in
        the list of |ingest_ready_normalized_file_paths| for the most recent date:
            - file has a daily update_cadence
            - file is referenced in any ingest view or BQ view in our codebase
        """
        configured_daily_file_tags = {
            file_tag
            for file_tag, config in region_raw_file_config.raw_file_configs.items()
            if config.update_cadence == RawDataFileUpdateCadence.DAILY
        }

        if not configured_daily_file_tags:
            # If there aren't any DAILY files configured, then we shouldn't be using this method
            raise ValueError(
                f"No `update_cadence: DAILY` file tags configured for [{region_raw_file_config.region_code}]"
            )

        referenced_file_tags = get_all_referenced_file_tags(
            state_code=StateCode(region_raw_file_config.region_code.upper())
        )
        required_daily_file_tags = configured_daily_file_tags & referenced_file_tags

        if not required_daily_file_tags:
            logging.info(
                "No DAILY file tags are currently referenced by any ingest view or BQ view in our codebase for region [%s]",
                region_raw_file_config.region_code,
            )
            return True

        return cls._are_all_required_files_present(
            region_code=region_raw_file_config.region_code,
            ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
            required_file_tags=required_daily_file_tags,
        )
