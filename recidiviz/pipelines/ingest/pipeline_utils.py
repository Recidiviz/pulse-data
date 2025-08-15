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

# The compute region (e.g. "us-east1") Dataflow pipelines for a given state should be run in.

# NOTE: Machine Type zonal availability can vary, and there is no guarantee that a machine is available across
# all zones within a region depending on Google Cloud's rollout

# NOTE: us-east7 and us-west8 do not seem to be zones that we can use, yet

# To find what zones have availability for a given machine, run:
# $ gcloud compute machine-types list --filter="name=c4a-highcpu-32" | grep "us-"
# c4a-highcpu-32  us-central1-a           32    64.00
# c4a-highcpu-32  us-central1-b           32    64.00
# c4a-highcpu-32  us-central1-c           32    64.00
# c4a-highcpu-32  us-west1-a              32    64.00
# c4a-highcpu-32  us-west1-c              32    64.00
# c4a-highcpu-32  us-east1-b              32    64.00
# c4a-highcpu-32  us-east1-c              32    64.00
# c4a-highcpu-32  us-east1-d              32    64.00
# c4a-highcpu-32  us-east4-a              32    64.00
# c4a-highcpu-32  us-east4-b              32    64.00
# c4a-highcpu-32  us-east4-c              32    64.00
# c4a-highcpu-32  us-east7-a              32    64.00
# c4a-highcpu-32  us-west8-c              32    64.00
DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE: Dict[StateCode, str] = {
    # us-east1 (3 zones w/ c4a-highcpu-32)
    StateCode.US_AR: "us-east1",
    StateCode.US_CA: "us-east1",
    StateCode.US_ME: "us-east1",
    StateCode.US_NC: "us-east1",
    StateCode.US_NE: "us-east1",
    StateCode.US_UT: "us-east1",
    # us-east4 (3 zones w/ c4a-highcpu-32)
    StateCode.US_CO: "us-east4",  # doesn't run
    StateCode.US_IA: "us-east4",
    StateCode.US_ID: "us-east4",  # doesn't run
    StateCode.US_IX: "us-east4",
    StateCode.US_OR: "us-east4",  # doesn't run
    StateCode.US_OZ: "us-east4",
    StateCode.US_PA: "us-east4",
    # us-central1 (3 zones w/ c4a-highcpu-32)
    StateCode.US_AZ: "us-central1",
    StateCode.US_MA: "us-central1",
    StateCode.US_MI: "us-central1",
    StateCode.US_NY: "us-central1",
    StateCode.US_TX: "us-central1",
    # us-west1 (2 zones w/ c4a-highcpu-32)
    StateCode.US_MO: "us-west1",
    StateCode.US_ND: "us-west1",
    StateCode.US_TN: "us-west1",
}
