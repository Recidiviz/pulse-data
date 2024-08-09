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
from typing import Any, Dict, List

import attr

from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

KNOWN_VALUES_CHECK_TEMPLATE = """
SELECT *
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL AND {column_name} NOT IN ({known_values})
LIMIT 1
"""


@attr.define
class KnownValuesColumnValidation(RawDataTableImportBlockingValidation):
    """Validation that checks if a column has values that are not one of the known_values supplied in the column config
    validation runs on all columns with supplied known_values unless explicitly exempt.
    """

    column_name: str
    known_values: List[str]
    validation_type: RawDataTableImportBlockingValidationType = (
        RawDataTableImportBlockingValidationType.KNOWN_VALUES
    )

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
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataTableImportBlockingValidationFailure | None:
        if results:
            # At least one row found with a value not in the known_values set
            return RawDataTableImportBlockingValidationFailure(
                validation_type=self.validation_type,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching any of the known_values defined in its configuration YAML.."
                    f"\nDefined known values: [{', '.join(self.known_values)}]."
                    f"\nFirst value that does not parse: [{results[0][self.column_name]}]."
                    f"\n Validation query: {self.query}"
                ),
            )
        # All rows have values in the known_values set
        return None
