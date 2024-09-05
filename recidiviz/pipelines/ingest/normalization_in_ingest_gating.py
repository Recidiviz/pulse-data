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
"""Gating helpers for rolling out normalization in ingest pipelines."""
from recidiviz.common.constants.states import StateCode
from recidiviz.utils import environment


# TODO(#31741): Delete this function once we've shipped combined ingest and
#  normalization
def should_run_normalization_in_ingest(state_code: StateCode) -> bool:
    """This gate determines whether we should output normalized entities from the ingest
    pipeline but does not determine whether we read that data downstream.
    """
    # TODO(#29517): Add states here as we launch combined pipelines to prod
    prod_launched_states: set[StateCode] = {
        StateCode.US_IA,
        StateCode.US_ID,
        StateCode.US_OZ,
        StateCode.US_TX,
    }
    if environment.in_gcp_production():
        return state_code in prod_launched_states
    staging_launched_states: set[StateCode] = {
        *prod_launched_states,
        StateCode.US_AZ,
        StateCode.US_NC,
        # TODO(#29517): Add states here as we launch combined pipelines to staging
    }
    return state_code in staging_launched_states


# TODO(#31741): Delete this function once we've shipped combined ingest and
#  normalization
def is_combined_ingest_and_normalization_launched_in_env(state_code: StateCode) -> bool:
    """This gate determines whether downstream processes read from the ingest pipeline
    normalization output.
    """
    # TODO(#29517): Add states here as we launch combined pipelines to prod
    prod_launched_states: set[StateCode] = {
        StateCode.US_IA,
        StateCode.US_ID,
        StateCode.US_OZ,
        StateCode.US_TX,
    }
    if environment.in_gcp_production():
        return state_code in prod_launched_states
    staging_launched_states: set[StateCode] = {
        *prod_launched_states,
        # TODO(#29517): Add states here as we launch combined pipelines to staging
    }
    return state_code in staging_launched_states
