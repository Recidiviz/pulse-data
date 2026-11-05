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
"""BigQuery names for the identity_service_export dataset: the dataset id and
the computed legacy id columns on its identities table.

These live apart from identity_service_export_source_tables.py, which defines
the full table schemas, so that readers of the export that only need its names
(e.g. activity pipeline parameters, which the calculation DAG entrypoint
imports) can reference them without depending on the source-table framework.
"""

IDENTITY_SERVICE_EXPORT_DATASET_ID = "identity_service_export"

LEGACY_PERSON_ID_COLUMN = "legacy_person_id"
LEGACY_STAFF_ID_COLUMN = "legacy_staff_id"
