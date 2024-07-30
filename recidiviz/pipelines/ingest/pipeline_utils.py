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
from typing import Dict

from recidiviz.common.constants.states import StateCode

# The compute region (e.g. "us-east1") Dataflow pipelines for a given state should be
#  run in.
DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE: Dict[StateCode, str] = {
    StateCode.US_AR: "us-east1",
    StateCode.US_CA: "us-east1",
    StateCode.US_CO: "us-west3",
    StateCode.US_IA: "us-west3",
    StateCode.US_ID: "us-west3",
    StateCode.US_IX: "us-west3",
    StateCode.US_MI: "us-central1",
    StateCode.US_MA: "us-central1",
    StateCode.US_ME: "us-east1",
    StateCode.US_MO: "us-central1",
    StateCode.US_NC: "us-east1",
    StateCode.US_ND: "us-east1",
    StateCode.US_OZ: "us-west3",
    StateCode.US_OR: "us-west3",
    StateCode.US_PA: "us-west3",
    StateCode.US_AZ: "us-central1",
    StateCode.US_TN: "us-central1",
}
