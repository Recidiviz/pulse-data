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
"""Helper functions related to ingest pipelines."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def ingest_pipeline_name(state_code: StateCode, instance: DirectIngestInstance) -> str:
    """Default ingest pipeline job_name to use for a given state/instance."""
    state_code_part = state_code.value.lower().replace("_", "-")
    instance_part = "-secondary" if instance is DirectIngestInstance.SECONDARY else ""
    pipeline_type_part = "-ingest"
    return f"{state_code_part}{pipeline_type_part}{instance_part}"