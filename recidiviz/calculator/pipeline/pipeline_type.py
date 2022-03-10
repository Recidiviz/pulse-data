# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Stores enums related to running calculation pipelines"""
import enum

# Normalization pipelines
COMPREHENSIVE_NORMALIZATION_PIPELINE_NAME = "COMPREHENSIVE_NORMALIZATION"

# Metrics pipelines
INCARCERATION_METRICS_PIPELINE_NAME = "INCARCERATION_METRICS"
PROGRAM_METRICS_PIPELINE_NAME = "PROGRAM_METRICS"
RECIDIVISM_METRICS_PIPELINE_NAME = "RECIDIVISM_METRICS"
SUPERVISION_METRICS_PIPELINE_NAME = "SUPERVISION_METRICS"
VIOLATION_METRICS_PIPELINE_NAME = "VIOLATION_METRICS"

# Supplemental dataset pipelines
US_ID_CASE_NOTE_EXTRACTED_ENTITIES_PIPELINE_NAME = (
    "US_ID_CASE_NOTE_EXTRACTED_ENTITIES_SUPPLEMENTAL"
)


class PipelineRunType(enum.Enum):
    """Describes the types of pipeline runs that occur."""

    # Pipelines scheduled to run daily
    INCREMENTAL = "INCREMENTAL"
    # Historical pipelines that only run on deploys
    HISTORICAL = "HISTORICAL"
