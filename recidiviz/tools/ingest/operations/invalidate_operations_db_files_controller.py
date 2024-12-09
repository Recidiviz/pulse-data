# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Controller for invalidating files in the operations database."""
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import attr
from sqlalchemy import text

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.log_helpers import make_log_output_path
from recidiviz.utils.string import StrictStringFormatter

SELECT_FILES_QUERY = """
SELECT file_tag, file_id, normalized_file_name, gcs_file_id
FROM direct_ingest_raw_gcs_file_metadata
WHERE region_code = '{region_code}' 
    AND raw_data_instance = '{raw_data_instance}' 
    AND is_invalidated IS NOT True
    {file_tag_clause}
    {optional_date_filter}
"""

UPDATE_BQ_METADATA_QUERY = """
UPDATE direct_ingest_raw_big_query_file_metadata
SET is_invalidated = True
WHERE file_id in ({file_ids})
"""

UPDATE_GCS_METADATA_QUERY = """
UPDATE direct_ingest_raw_gcs_file_metadata
SET is_invalidated = True
WHERE gcs_file_id in ({gcs_file_ids})
"""


@attr.define
class RawFilesGroupedByTagAndId:
    """Represents a group of raw files grouped by file tag, file id and gcs file id.
    Args:
        file_tag_to_file_id_dict: A dictionary mapping file tags to dictionaries that map file ids to lists of GCS file ids.
            so formatted as Dict[file_tag, Dict[file_id, List[gcs_file_id]]]. This is stored as a dictionary of dictionaries
            (instead of two dictionaries like Dict[file_tag, List[file_ids]] and Dict[file_id, List[gcs_file_ids]]) because a file tag may have
            entries in direct_ingest_raw_gcs_file_metadata that do not have a file_id or an entry in direct_ingest_raw_big_query_file_metadata
            (like in the case of ungrouped file chunks). In that case, there might be multiple gcs_file_ids with file_id=None that have
            different file tags, so we can't key on file_id independent of file_tag.
        gcs_file_id_to_file_name: A dictionary mapping GCS file ids to normalized file names.
    """

    file_tag_to_file_id_dict: defaultdict[
        str, defaultdict[Optional[int], List[int]]
    ] = attr.ib(validator=attr.validators.instance_of(defaultdict))
    gcs_file_id_to_file_name: Dict[int, str] = attr.ib(
        validator=attr_validators.is_dict
    )

    @classmethod
    def from_file_tag_id_name_tuples(
        cls, tuple_list: List[Tuple[str, Optional[int], str, int]]
    ) -> "RawFilesGroupedByTagAndId":
        file_tag_to_file_ids: defaultdict[
            str, defaultdict[Optional[int], List[int]]
        ] = defaultdict(lambda: defaultdict(list))
        gcs_file_id_to_file_name = {
            gcs_file_id: normalized_file_name
            for _, _, normalized_file_name, gcs_file_id in tuple_list
        }
        for file_tag, file_id, _, gcs_file_id in tuple_list:
            file_tag_to_file_ids[file_tag][file_id].append(gcs_file_id)

        return cls(file_tag_to_file_ids, gcs_file_id_to_file_name)

    def __str__(self) -> str:
        output = []
        for file_tag, file_ids in self.file_tag_to_file_id_dict.items():
            output.append(f"{file_tag}:")
            for file_id, gcs_file_ids in file_ids.items():
                output.append(f"  file_id: {file_id}")
                for gcs_id in gcs_file_ids:
                    output.append(
                        f"    - gcs_file_id {gcs_id}: {self.gcs_file_id_to_file_name[gcs_id]}"
                    )
        return "\n".join(output)

    def empty(self) -> bool:
        return not any(self.file_tag_to_file_id_dict.values())

    @property
    def file_ids(self) -> Set[int]:
        return {
            file_id
            for files in self.file_tag_to_file_id_dict.values()
            for file_id in files
            if file_id
        }

    @property
    def gcs_file_ids(self) -> Set[int]:
        return {
            gcs_file_id
            for files in self.file_tag_to_file_id_dict.values()
            for file_ids in files.values()
            for gcs_file_id in file_ids
        }

    @property
    def normalized_file_names(self) -> Set[str]:
        return set(self.gcs_file_id_to_file_name.values())

    @property
    def file_tag_to_file_ids(self) -> Dict[str, List[int]]:
        return {
            file_tag: list(filter(None, file_ids.keys()))
            for file_tag, file_ids in self.file_tag_to_file_id_dict.items()
        }


@attr.define
class InvalidateOperationsDBFilesController:
    """Invalidates entries in the operations db corresponding to the files that match the provided filters.
    This class only supports updating operations tables for states/ingest_instances with the raw data import DAG enabled.

    Args:
        project_id: The GCP project ID.
        state_code: The state code.
        ingest_instance: The ingest instance.
        file_tag_filters: A list of file tags to filter by.
        file_tag_regex: A regex pattern to filter by.
        start_date_bound: The isoformatted start date bound for the update_datetime column.
        end_date_bound: The isoformatted end date bound for the update_datetime column.
        dry_run: Whether to perform a dry run.
        skip_prompts: Whether to skip confirmation prompts.
    """

    project_id: str = attr.ib(validator=attr_validators.is_str)
    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    ingest_instance: DirectIngestInstance = attr.ib(
        validator=attr.validators.instance_of(DirectIngestInstance)
    )
    file_tag_filters: List[str] = attr.ib(validator=attr_validators.is_list)
    file_tag_regex: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    start_date_bound: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    end_date_bound: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    dry_run: bool = attr.ib(default=True, validator=attr_validators.is_bool)
    skip_prompts: bool = attr.ib(default=False, validator=attr_validators.is_bool)
    log_output_path: str = attr.ib(init=False, validator=attr_validators.is_str)

    def __attrs_post_init__(self) -> None:
        if not is_raw_data_import_dag_enabled(self.state_code, self.ingest_instance):
            raise ValueError(
                "Invalidation operation only supports updating operations tables "
                "for states/ingest_instances with the raw data import DAG enabled."
            )
        if self.file_tag_filters and self.file_tag_regex:
            raise ValueError(
                "Cannot provide both file_tag_filters and file_tag_regex. Please provide only one."
            )
        self.log_output_path = make_log_output_path(
            operation_name="invalidate_operations_db_files",
            region_code=self.state_code.value,
            date_string=f"start_bound_{self.start_date_bound}_end_bound_{self.end_date_bound}",
            dry_run=self.dry_run,
        )

    def _write_log_file(
        self, log_output_path: str, parsed_results: RawFilesGroupedByTagAndId
    ) -> None:
        with open(log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                f.write("[DRY RUN] Would invalidate the following files:\n")
            else:
                f.write("Invalidated the following files:\n")
            f.write(str(parsed_results))

    def _execute_invalidation(
        self, session: Session, file_ids: Set[int], gcs_file_ids: Set[int]
    ) -> None:
        """Executes invalidation by updating metadata tables."""
        bq_metadata_query_str = StrictStringFormatter().format(
            UPDATE_BQ_METADATA_QUERY,
            file_ids=", ".join(map(str, file_ids)),
        )
        gcs_metadata_query_str = StrictStringFormatter().format(
            UPDATE_GCS_METADATA_QUERY,
            gcs_file_ids=", ".join(map(str, gcs_file_ids)),
        )

        session.execute(text(bq_metadata_query_str))
        session.execute(text(gcs_metadata_query_str))

        session.commit()

    def _get_datetime_filter_clause(self) -> str:
        datetime_filter_clause = ""
        if self.start_date_bound:
            datetime_filter_clause = (
                f"AND DATE(update_datetime) >= '{self.start_date_bound}' "
            )
        if self.end_date_bound:
            datetime_filter_clause += (
                f"AND DATE(update_datetime) <= '{self.end_date_bound}'"
            )
        return datetime_filter_clause

    def _get_file_tag_clause(self) -> str:
        if self.file_tag_filters:
            file_tag_str = ",".join(f"'{tag}'" for tag in self.file_tag_filters)
            return f"AND file_tag IN ({file_tag_str})"
        if self.file_tag_regex:
            return f"AND file_tag ~ '{self.file_tag_regex}'"
        return ""

    def _fetch_files_to_be_invalidated(
        self, session: Session
    ) -> RawFilesGroupedByTagAndId:
        query_str = StrictStringFormatter().format(
            SELECT_FILES_QUERY,
            region_code=self.state_code.value,
            raw_data_instance=self.ingest_instance.value,
            file_tag_clause=self._get_file_tag_clause(),
            optional_date_filter=self._get_datetime_filter_clause(),
        )
        result = session.execute(text(query_str))

        return RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(result.fetchall())

    def run(self) -> Optional[RawFilesGroupedByTagAndId]:
        """This operation will update the is_invalidated column in the direct_ingest_raw_big_query_file_metadata
        and direct_ingest_raw_gcs_file_metadata tables to True for the rows that match the relevant filters.
        Returns a RawFilesGroupedByTagAndId object representing the files that were invalidated or None if no
        files were invalidated.
        """
        schema_type = SchemaType.OPERATIONS
        database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
        with cloudsql_proxy_control.connection(
            schema_type=schema_type,
        ):
            with SessionFactory.for_proxy(
                database_key=database_key,
                autocommit=False,
            ) as session:
                files_to_be_invalidated = self._fetch_files_to_be_invalidated(session)
                if files_to_be_invalidated.empty():
                    logging.info("No files to invalidate.")
                    return None

                if not self.skip_prompts:
                    prompt_for_confirmation(
                        f"This operation will invalidate [{len(files_to_be_invalidated.normalized_file_names)}] files.",
                        accepted_response_override="yes",
                        dry_run=self.dry_run,
                    )

                if not self.dry_run:
                    self._execute_invalidation(
                        session,
                        file_ids=files_to_be_invalidated.file_ids,
                        gcs_file_ids=files_to_be_invalidated.gcs_file_ids,
                    )

                invalidated_files = files_to_be_invalidated
                self._write_log_file(self.log_output_path, invalidated_files)
                if self.dry_run:
                    logging.info(
                        "[DRY RUN] See results in [%s].\n"
                        "Rerun with [--dry-run False] to execute invalidation.",
                        self.log_output_path,
                    )
                else:
                    logging.info(
                        "Invalidation complete! See results in [%s].\n",
                        self.log_output_path,
                    )
                return invalidated_files
