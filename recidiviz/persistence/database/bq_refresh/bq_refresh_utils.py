# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for interacting with GCS."""

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType

POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME = "EXPORT_PROCESS_RUNNING"


def postgres_to_bq_lock_name_for_schema(
    schema: SchemaType, ingest_instance: DirectIngestInstance
) -> str:
    return f"{POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME}_{schema.value.upper()}_{ingest_instance.value.upper()}"
