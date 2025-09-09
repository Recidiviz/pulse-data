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
import datetime
from typing import Any, ClassVar

import attr

from recidiviz.big_query.big_query_utils import escape_backslashes_for_query
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataColumnValidationMixin,
    RawDataImportBlockingValidationContext,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

ERROR_MESSAGE_ROW_LIMIT = 10

KNOWN_VALUES_TEMPLATE = """
WITH failed_known_values AS (
  SELECT {case_when_statements}
  FROM `{project_id}.{dataset_id}.{table_id}`
),
aggregated_failures AS (
  SELECT {aggregation_statements}
  FROM failed_known_values
)
SELECT
  column_name,
  failed_values
FROM aggregated_failures
UNPIVOT (
  failed_values FOR column_name IN ({column_mapping})
)
WHERE ARRAY_LENGTH(failed_values) > 0
"""


@attr.define
class KnownValuesValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Validation that checks if any columns in a given file tag have values that are not one of the known_values supplied in their column config.
    Validation runs on all columns with supplied `known_values` unless explicitly exempt.
    """

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.KNOWN_VALUES
    column_name_to_columns: dict[str, RawTableColumnInfo]

    def __attrs_post_init__(self) -> None:
        self._ensure_non_empty_known_values()

    def _ensure_non_empty_known_values(self) -> None:
        for column_name, column in self.column_name_to_columns.items():
            if not column.known_values:
                raise ValueError(f"known_values for [{column_name}] must not be empty")

    @classmethod
    def _get_columns_to_validate(
        cls,
        raw_file_config: DirectIngestRawFileConfig,
        file_update_datetime: datetime.datetime,
    ) -> list[RawTableColumnInfo]:
        return [
            col
            for col in cls.get_columns_relevant_for_validation(
                cls.VALIDATION_TYPE, raw_file_config, file_update_datetime
            )
            if col.known_values
        ]

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "KnownValuesValidation":
        columns_to_validate = cls._get_columns_to_validate(
            context.raw_file_config, file_update_datetime=context.file_update_datetime
        )
        if not columns_to_validate:
            raise ValueError(
                f"No columns with configured known_values in [{context.file_tag}]"
            )

        return cls(
            project_id=context.project_id,
            temp_table_address=context.temp_table_address,
            file_tag=context.file_tag,
            state_code=context.state_code,
            column_name_to_columns={
                assert_type(
                    col.name_at_datetime(context.file_update_datetime), str
                ): col
                for col in columns_to_validate
            },
        )

    @classmethod
    def validation_applies_to_file(
        cls, context: RawDataImportBlockingValidationContext
    ) -> bool:
        """Returns True if the file is not exempt from the validation and there is at least one column with
        configured known_values that is not exempt from the validation for the given file config
        """
        return bool(
            cls._get_columns_to_validate(
                context.raw_file_config, context.file_update_datetime
            )
        )

    def build_query(self) -> str:
        formatter = StrictStringFormatter()

        case_when_statements: list[str] = []
        aggregation_statements: list[str] = []
        column_mapping: list[str] = []

        for column_name, column in self.column_name_to_columns.items():
            known_values_str = ", ".join(
                [
                    f'"{value}"'
                    for value in escape_backslashes_for_query(
                        [v.value for v in assert_type(column.known_values, list)]
                    )
                ]
            )
            null_values_filter = self.build_null_values_filter(
                column.name, column.null_values
            )

            # CASE statement to pick out invalid values
            case_when_statements.append(
                f"""
            CASE
            WHEN {column_name} IS NOT NULL
                AND {column_name} NOT IN ({known_values_str})
                {null_values_filter}
            THEN CAST({column_name} AS STRING)
            ELSE NULL
            END AS {column_name}_failed_value"""
            )

            # ARRAY_AGG to collect invalid values
            aggregation_statements.append(
                f"""
            ARRAY_AGG(DISTINCT {column_name}_failed_value IGNORE NULLS LIMIT {ERROR_MESSAGE_ROW_LIMIT})
            AS {column_name}_failures"""
            )

            # UNPIVOT mapping
            column_mapping.append(f"{column_name}_failures AS '{column_name}'")

        return formatter.format(
            KNOWN_VALUES_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            case_when_statements=",".join(case_when_statements),
            aggregation_statements=",\n    ".join(aggregation_statements),
            column_mapping=", ".join(column_mapping),
        )

    def _get_relevancy_error_message(self, column_name: str) -> str:
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
                f"list of known_values for [{column_name}] if you want to keep "
                f"the addition of new values as import-blocking; (2) add an import-blocking "
                f"exclusion if you want to keep the existing documentation but know that "
                f"it may quickly become stale; or (3) remove all known_values for "
                f"[{column_name}] from the yaml config to remove its designation as "
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
            f"add the new values to list of known_values for [{column_name}] if you want to keep "
            f"the addition of new values as import-blocking; (2) add an import-blocking "
            f"exclusion if you want to keep the existing documentation but know that "
            f"it may quickly become stale; or (3) remove all known_values for "
            f"[{column_name}] from the yaml config to remove its designation as "
            f"an enum."
        )

    def get_error_from_results(
        self,
        results: list[dict[str, Any]],
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            return None

        error_msg = (
            f"Found column(s) on raw file [{self.file_tag}] "
            "not matching any of the known_values defined in its configuration YAML."
        )
        for result in results:
            column_name = result["column_name"]
            failed_values = result["failed_values"]
            column = self.column_name_to_columns[column_name]

            quoted_known_values = [
                f'"{v.value}"' for v in assert_type(column.known_values, list)
            ]
            quoted_failed_values = [f'"{v}"' for v in failed_values]
            relevancy_error_msg = self._get_relevancy_error_message(column_name)
            error_msg += (
                f"\nColumn name: [{column_name}]"
                f"\nDefined known values: [{', '.join(quoted_known_values)}]."
                f"\nValues that did not parse: [{', '.join(quoted_failed_values)}]."
                f"\n{relevancy_error_msg}\n"
            )

        return RawDataImportBlockingValidationFailure(
            validation_type=self.VALIDATION_TYPE,
            validation_query=self.build_query(),
            error_msg=error_msg,
        )
