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
"""Validation to check if a column has values that are not one of the known_values supplied in the column config."""
from datetime import datetime
from typing import Any, Dict, List, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.utils.string import StrictStringFormatter

KNOWN_VALUES_CHECK_TEMPLATE = """
SELECT DISTINCT {column_name}
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
    AND {column_name} NOT IN ({known_values})
    {null_values_filter}
"""


@attr.define
class KnownValuesColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation that checks if a column has values that are not one of the known_values supplied in the column config
    validation runs on all columns with supplied known_values unless explicitly exempt.
    """

    known_values: List[str]
    null_values: Optional[List[str]]

    @staticmethod
    def _get_escaped_known_values(known_values: List[ColumnEnumValueInfo]) -> List[str]:
        return [known_value.value.replace("\\", "\\\\") for known_value in known_values]

    @classmethod
    def create_column_validation(
        cls,
        *,
        file_tag: str,
        project_id: str,
        state_code: StateCode,
        temp_table_address: BigQueryAddress,
        file_upload_datetime: datetime,
        column: RawTableColumnInfo,
    ) -> "KnownValuesColumnValidation":
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )
        if not column.known_values:
            raise ValueError(f"known_values for {column.name} must not be empty")

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            column_name=temp_table_col_name,
            known_values=cls._escape_values_for_query(
                [enum.value for enum in column.known_values]
            ),
            null_values=(
                cls._escape_values_for_query(column.null_values)
                if column.null_values
                else None
            ),
            state_code=state_code,
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.KNOWN_VALUES

    @staticmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo,
        _raw_file_config: DirectIngestRawFileConfig,
    ) -> bool:
        return column.known_values is not None

    def build_query(self) -> str:
        if not self.known_values:
            raise ValueError(f"known_values for {self.column_name} must not be empty")

        return StrictStringFormatter().format(
            KNOWN_VALUES_CHECK_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_name=self.column_name,
            known_values=", ".join([f'"{value}"' for value in self.known_values]),
            null_values_filter=self._build_null_values_filter(
                self.column_name, self.null_values
            ),
        )

    def _get_relevancy_error_message(self) -> str:
        """Fetches and formats an error message about whether or not there are ingest
        views that reference the file tag associated with this validation.
        """
        # TODO(#40421): filter to just relevancy if this column is actually used in an
        # ingest enum mapping, and say which mapping it is
        referencing_ingest_views = sorted(
            DirectIngestViewQueryBuilderCollector.from_state_code(
                state_code=self.state_code
            ).get_ingest_views_referencing(self.file_tag)
        )

        if not referencing_ingest_views:
            return (
                f"No ingest views references [{self.file_tag}]. To resolve this error, you can: (1) update the "
                f"list of known_values for [{self.column_name}] if you want to keep "
                f"the addition of new values as import-blocking; (2) add an import-blocking "
                f"exclusion if you want to keep the existing documentation but know that "
                f"it may quickly become stale; or (3) remove all known_values for "
                f"[{self.column_name}] from the yaml config to remove its designation as "
                f"an enum."
            )

        referencing_ingest_views_str = "\n".join(
            f"\t-{ingest_view}" for ingest_view in referencing_ingest_views
        )
        return (
            f"The following ingest views reference [{self.file_tag}]:"
            f"\n{referencing_ingest_views_str}\nIf this column is used in an ingest enum "
            f"mapping, adding the new values will help ensure that the enum failure "
            f"occurs at raw data import time instead of ingest time. If it is not used, you can (1) "
            f"add the new values to list of known_values for [{self.column_name}] if you want to keep "
            f"the addition of new values as import-blocking; (2) add an import-blocking "
            f"exclusion if you want to keep the existing documentation but know that "
            f"it may quickly become stale; or (3) remove all known_values for "
            f"[{self.column_name}] from the yaml config to remove its designation as "
            f"an enum."
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            missing_values = [f'"{result[self.column_name]}"' for result in results]
            quoted_known_values = [f'"{v}"' for v in self.known_values]
            relevancy_error_msg = self._get_relevancy_error_message()
            # At least one row found with a value not in the known_values set
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type(),
                validation_query=self.query,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching any of the known_values defined in its configuration YAML."
                    f"\nDefined known values: [{', '.join(quoted_known_values)}]."
                    f"\nValues that did not parse: [{', '.join(missing_values)}]."
                    f"\n{relevancy_error_msg}"
                ),
            )
        # All rows have values in the known_values set
        return None
