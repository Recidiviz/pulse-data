# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for BQ refresh"""
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType


def postgres_to_bq_lock_name_with_suffix(suffix: str) -> str:
    return POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME + suffix.upper()


def postgres_to_bq_lock_name_for_schema(schema: SchemaType) -> str:
    return postgres_to_bq_lock_name_with_suffix(schema.value)
